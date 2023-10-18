// Copyright © 2023 Kaleido, Inc.
//
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package eventstreams

import (
	"context"
	"sync"
	"time"

	"github.com/hyperledger/firefly-common/pkg/dbsql"
	"github.com/hyperledger/firefly-common/pkg/fftypes"
	"github.com/hyperledger/firefly-common/pkg/i18n"
	"github.com/hyperledger/firefly-common/pkg/log"
	"github.com/hyperledger/firefly-transaction-manager/pkg/apitypes"
)

type eventStreamBatch struct {
	number     int64
	events     []*Event
	batchTimer *time.Timer
}

type activeStream[CT any] struct {
	*eventStream[CT]
	ctx           context.Context
	cancelCtx     func()
	batchNumber   int64
	filterSkipped int64
	EventStreamStatistics
	eventLoopDone chan struct{}
	batchLoopDone chan struct{}
	events        chan *Event

	checkpointLock       sync.Mutex
	dispatchedCheckpoint string
	queuedCheckpoint     string
}

func (es *eventStream[CT]) newActiveStream() *activeStream[CT] {
	ctx, cancelCtx := context.WithCancel(es.bgCtx)
	as := &activeStream[CT]{
		ctx:       ctx,
		cancelCtx: cancelCtx,
		EventStreamStatistics: EventStreamStatistics{
			StartTime: fftypes.Now(),
		},
		eventLoopDone: make(chan struct{}),
		batchLoopDone: make(chan struct{}),
		events:        make(chan *Event, *es.spec.BatchSize),
	}
	go as.runEventLoop()
	go as.runBatchLoop()
	return as
}

func (as *activeStream[CT]) runEventLoop() {
	defer close(as.eventLoopDone)

	// Read the last checkpoint for this stream
	cp, err := as.loadCheckpoint()
	if err == nil {
		// Run the inner source read loop until it exits
		err = as.retry.Do(as.ctx, "source run loop", func(attempt int) (retry bool, err error) {
			if err = as.runSourceLoop(*cp.SequenceID); err == nil {
				log.L(as.ctx).Errorf("source loop error: %s", err)
				return true, err
			}
			// the Run loop must only exit with nil error if the context is closed
			// (which we also signal with an Exit instruction)
			return false, nil
		})

	}
	// Retry will only return an error if the context is cancelled
	log.L(as.ctx).Debugf("event loop exiting (%s)", err)
}

func (as *activeStream[CT]) loadCheckpoint() (cp *EventStreamCheckpoint, err error) {
	err = as.retry.Do(as.ctx, "load checkpoint", func(attempt int) (retry bool, err error) {
		cp, err = as.persistence.Checkpoints().GetByID(as.ctx, as.spec.ID.String(), dbsql.FailIfNotFound)
		return true, err
	})
	return cp, err
}

func (as *activeStream[CT]) checkFilter(event *Event) bool {
	if as.spec.topicFilterRegexp != nil {
		return as.spec.topicFilterRegexp.Match([]byte(event.Topic))
	}
	return true
}

func (as *activeStream[CT]) runSourceLoop(initialCheckpointSequenceID string) error {
	// Responsibility of the source to block until events are available, or the context is closed.
	return as.esm.runtime.Run(as.ctx, as.spec, initialCheckpointSequenceID, func(events []*Event) SourceInstruction {
		// There's no direct connection between any batching used in the source routine,
		// and our batch based delivery. This is intentional - allowing separate optimization
		// of each routine for the source data store/stream.
		for _, event := range events {
			select {
			case as.events <- event:
			case <-as.ctx.Done():
				// Event stream has has shut down
				return Exit
			}
		}

		// Explicitly check for done here, as the above doesn't assure we'd trigger
		select {
		case <-as.ctx.Done():
			return Exit
		default:
		}

		// Instruct the run loop to continue
		return Continue
	})

}

func (as *activeStream[CT]) runBatchLoop() {
	defer close(as.batchLoopDone)

	var batch *eventStreamBatch
	batchTimeout := time.Duration(*as.spec.BatchTimeout)
	var noBatchActive <-chan time.Time = make(chan time.Time) // never pops
	batchTimedOut := noBatchActive
	for {

		// Pull events out of the event loop, and assemble them into batches with a max + timeout
		var timedOut = false
		select {
		case <-as.ctx.Done():
			log.L(as.ctx).Debugf("batch loop done")
			return
		case <-batchTimedOut:
			timedOut = true
		case event := <-as.events:
			as.HighestDetected = event.SequenceID
			if !as.checkFilter(event) {
				as.filterSkipped++
			} else {
				if batch == nil {
					as.batchNumber++
					batch = &eventStreamBatch{
						number:     as.batchNumber,
						batchTimer: time.NewTimer(batchTimeout),
						events:     make([]*Event, 0, *as.spec.BatchSize),
					}
					batchTimedOut = batch.batchTimer.C
				}
				batch.events = append(batch.events, event)
			}
		}
		batchDispatched := false
		if batch != nil && (len(batch.events) >= *as.spec.BatchSize || timedOut) {
			// attempt dispatch (only returns err on exit)
			if err := as.dispatchBatch(batch); err != nil {
				log.L(as.ctx).Debugf("batch loop done: %s", err)
				return
			}
			batchDispatched = true
			// reset batch
			batch.batchTimer.Stop()
			batchTimedOut = noBatchActive
			batch = nil
		}
		if batchDispatched || as.filterSkipped > as.esm.config.Checkpoints.UnmatchedEventThreshold {
			// At this point we are sure that the highest detected event, is above the highest
			// acknowledged event.
			as.dispatchCheckpoint()
			// Reset our skip tracker
			as.filterSkipped = 0
		}

	}
}

func (as *activeStream[CT]) dispatchCheckpoint() {
	if as.pushCheckpoint() {
		if as.esm.config.Checkpoints.Asynchronous {
			go as.checkpointRoutine() // async
		} else {
			as.checkpointRoutine() // in-line
		}
	}
}

func (as *activeStream[CT]) pushCheckpoint() bool {
	as.checkpointLock.Lock()
	defer as.checkpointLock.Unlock()
	if as.dispatchedCheckpoint == "" {
		as.dispatchedCheckpoint = as.HighestDetected
		return true // we need to run the checkpoint worker
	}
	as.queuedCheckpoint = as.HighestDetected
	return false // it'll be picked up before the existing worker ends
}

func (as *activeStream[CT]) popCheckpoint() string {
	as.checkpointLock.Lock()
	defer as.checkpointLock.Unlock()
	as.dispatchedCheckpoint = as.queuedCheckpoint
	return as.dispatchedCheckpoint
}

func (as *activeStream[CT]) checkpointRoutine() {
	for {
		checkpointSequenceID := as.popCheckpoint()
		if checkpointSequenceID == "" {
			return // We're done
		}
		err := as.retry.Do(as.ctx, "checkpoint", func(attempt int) (retry bool, err error) {
			_, err = as.esm.persistence.Checkpoints().Upsert(as.ctx, &EventStreamCheckpoint{
				ResourceBase: dbsql.ResourceBase{
					ID: as.spec.ID, // the ID of the stream is the ID of the checkpoint
				},
				SequenceID: &checkpointSequenceID,
			}, dbsql.UpsertOptimizationExisting)
			return true, err
		})
		if err != nil {
			// must be cancelled context
			log.L(as.ctx).Warnf("checkpoint cancelled: %s", err)
			return
		}
	}
}

// performActionWithRetry performs an action, with exponential back-off retry up
// to a given threshold. Only returns error in the case that the context is closed.
func (as *activeStream[CT]) dispatchBatch(batch *eventStreamBatch) (err error) {
	as.LastDispatchNumber = batch.number
	as.LastDispatchTime = fftypes.Now()
	as.LastDispatchFailure = ""
	as.LastDispatchAttempts = 0
	as.LastDispatchStatus = DispatchStatusDispatching
	as.HighestDispatched = batch.events[len(batch.events)-1].SequenceID
	for {
		// Short exponential back-off retry
		err := as.retry.Do(as.ctx, "action", func(_ int) (retry bool, err error) {
			err = as.action.attemptDispatch(as.ctx, batch.number, as.LastDispatchAttempts, batch.events)
			if err != nil {
				log.L(as.ctx).Errorf("Batch %d attempt %d failed. err=%s",
					batch.number, as.LastDispatchAttempts, err)
				as.LastDispatchAttempts++
				as.LastDispatchFailure = err.Error()
				as.LastDispatchStatus = DispatchStatusRetrying
				return time.Since(*as.LastDispatchTime.Time()) < time.Duration(*as.spec.RetryTimeout), err
			}
			as.LastDispatchStatus = DispatchStatusComplete
			return false, nil
		})
		if err == nil {
			return nil
		}
		// We're in blocked retry delay
		as.LastDispatchStatus = DispatchStatusBlocked
		log.L(as.ctx).Errorf("Batch failed short retry after %.2fs secs. ErrorHandling=%s BlockedRetryDelay=%.2fs ",
			time.Since(*as.LastDispatchTime.Time()).Seconds(), *as.spec.ErrorHandling, time.Duration(*as.spec.BlockedRetryDelay).Seconds())
		if *as.spec.ErrorHandling == apitypes.ErrorHandlingTypeSkip {
			// Swallow the error now we have logged it
			as.LastDispatchStatus = DispatchStatusSkipped
			return nil
		}
		select {
		case <-time.After(time.Duration(*as.spec.BlockedRetryDelay)):
		case <-as.ctx.Done():
			// Only way we exit with error, is if the context is cancelled
			return i18n.NewError(as.ctx, i18n.MsgContextCanceled)
		}
	}
}
