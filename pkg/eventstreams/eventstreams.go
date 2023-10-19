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
	"crypto/tls"
	"database/sql"
	"database/sql/driver"
	"regexp"
	"sync"

	"github.com/hyperledger/firefly-common/pkg/dbsql"
	"github.com/hyperledger/firefly-common/pkg/fftypes"
	"github.com/hyperledger/firefly-common/pkg/i18n"
	"github.com/hyperledger/firefly-common/pkg/log"
	"github.com/hyperledger/firefly-common/pkg/retry"
)

type EventStreamType = fftypes.FFEnum

var (
	EventStreamTypeWebhook   = fftypes.FFEnumValue("estype", "webhook")
	EventStreamTypeWebSocket = fftypes.FFEnumValue("estype", "websocket")
)

type ErrorHandlingType = fftypes.FFEnum

var (
	ErrorHandlingTypeBlock = fftypes.FFEnumValue("ehtype", "block")
	ErrorHandlingTypeSkip  = fftypes.FFEnumValue("ehtype", "skip")
)

type DispatchStatus = fftypes.FFEnum

var (
	DispatchStatusDispatching = fftypes.FFEnumValue("edstatus", "dispatching")
	DispatchStatusRetrying    = fftypes.FFEnumValue("edstatus", "retrying")
	DispatchStatusBlocked     = fftypes.FFEnumValue("edstatus", "blocked")
	DispatchStatusComplete    = fftypes.FFEnumValue("edstatus", "complete")
	DispatchStatusSkipped     = fftypes.FFEnumValue("edstatus", "skipped")
)

type EventStreamStatus = fftypes.FFEnum

var (
	EventStreamStatusStarted         = fftypes.FFEnumValue("esstatus", "started")
	EventStreamStatusStopped         = fftypes.FFEnumValue("esstatus", "stopped")
	EventStreamStatusDeleted         = fftypes.FFEnumValue("esstatus", "deleted")
	EventStreamStatusStopping        = fftypes.FFEnumValue("esstatus", "stopping")         // not persisted
	EventStreamStatusStoppingDeleted = fftypes.FFEnumValue("esstatus", "stopping_deleted") // not persisted
	EventStreamStatusUnknown         = fftypes.FFEnumValue("esstatus", "unknown")          // not persisted
)

// Let's us check that the config serializes
type DBSerializable interface {
	sql.Scanner
	driver.Valuer
}

type EventStreamSpec[CT any] struct {
	dbsql.ResourceBase
	Name        *string            `ffstruct:"eventstream" json:"name,omitempty"`
	Status      *EventStreamStatus `ffstruct:"eventstream" json:"status,omitempty"`
	Type        *EventStreamType   `ffstruct:"eventstream" json:"type,omitempty" ffenum:"estype"`
	TopicFilter *string            `ffstruct:"eventstream" json:"topicFilter,omitempty" ffenum:"estype"`
	Config      *CT                `ffstruct:"eventstream" json:"config,omitempty"`

	ErrorHandling     *ErrorHandlingType  `ffstruct:"eventstream" json:"errorHandling"`
	BatchSize         *int                `ffstruct:"eventstream" json:"batchSize"`
	BatchTimeout      *fftypes.FFDuration `ffstruct:"eventstream" json:"batchTimeout"`
	RetryTimeout      *fftypes.FFDuration `ffstruct:"eventstream" json:"retryTimeout"`
	BlockedRetryDelay *fftypes.FFDuration `ffstruct:"eventstream" json:"blockedRetryDelay"`

	Webhook   *WebhookConfig   `ffstruct:"eventstream" json:"webhook,omitempty"`
	WebSocket *WebSocketConfig `ffstruct:"eventstream" json:"websocket,omitempty"`

	topicFilterRegexp *regexp.Regexp
}

func (esc *EventStreamSpec[CT]) GetID() string {
	return esc.ID.String()
}

func (esc *EventStreamSpec[CT]) SetCreated(t *fftypes.FFTime) {
	esc.Created = t
}

func (esc *EventStreamSpec[CT]) SetUpdated(t *fftypes.FFTime) {
	esc.Updated = t
}

type EventStreamStatistics struct {
	StartTime            *fftypes.FFTime `ffstruct:"EventStreamStatistics" json:"startTime"`
	LastDispatchTime     *fftypes.FFTime `ffstruct:"EventStreamStatistics" json:"lastDispatchTime"`
	LastDispatchNumber   int64           `ffstruct:"EventStreamStatistics" json:"lastDispatchBatch"`
	LastDispatchAttempts int             `ffstruct:"EventStreamStatistics" json:"lastDispatchAttempts,omitempty"`
	LastDispatchFailure  string          `ffstruct:"EventStreamStatistics" json:"lastDispatchFailure,omitempty"`
	LastDispatchStatus   DispatchStatus  `ffstruct:"EventStreamStatistics" json:"lastDispatchComplete"`
	HighestDetected      string          `ffstruct:"EventStreamStatistics" json:"highestDetected"`
	HighestDispatched    string          `ffstruct:"EventStreamStatistics" json:"highestDispatched"`
	Checkpoint           string          `ffstruct:"EventStreamStatistics" json:"checkpoint"`
}

type EventStreamWithStatus[CT any] struct {
	*EventStreamSpec[CT]
	Status     EventStreamStatus      `ffstruct:"EventStream" json:"status"`
	Statistics *EventStreamStatistics `ffstruct:"EventStream" json:"statistics,omitempty"`
}

type EventStreamCheckpoint struct {
	dbsql.ResourceBase
	SequenceID *string `ffstruct:"EventStreamCheckpoint" json:"sequenceId,omitempty"`
}

type EventBatchDispatcher[DT any] interface {
	AttemptDispatch(ctx context.Context, batchNumber int64, attempt int, events []*Event[DT]) error
}

func checkSet[T any](ctx context.Context, storeDefaults bool, fieldName string, fieldPtr **T, defValue T, check func(v T) bool) error {
	var v T
	if *fieldPtr != nil {
		v = **fieldPtr
	} else {
		if storeDefaults {
			*fieldPtr = &defValue
		}
		v = defValue
	}
	log.L(ctx).Tracef("Resolved value '%v' for field '%s'", v, fieldName)
	if !check(v) {
		return i18n.NewError(ctx, i18n.MsgInvalidValue, v, fieldName)
	}
	return nil
}

// Validate checks all the field values, once combined with defaults.
// Optionally it stores the defaults back on the structure, to ensure no nil fields.
// - When using at runtime: true, so later code doesn't need to worry about nil checks / defaults
// - When storing to the DB: false, so defaults can be applied dynamically
func (esc *EventStreamSpec[CT]) Validate(ctx context.Context, tlsConfigs map[string]*tls.Config, defaults *EventStreamDefaults, setDefaults bool) (err error) {
	if esc.Name == nil {
		return i18n.NewError(ctx, i18n.MsgMissingRequiredField, "name")
	}
	if esc.TopicFilter != nil {
		fullMatchFilter := `^` + *esc.TopicFilter + `$`
		if esc.topicFilterRegexp, err = regexp.Compile(fullMatchFilter); err != nil {
			return i18n.NewError(ctx, i18n.MsgESInvalidTopicFilterRegexp, fullMatchFilter, err)
		}
	}
	if err := fftypes.ValidateFFNameField(ctx, *esc.Name, "name"); err != nil {
		return err
	}
	if err := checkSet(ctx, setDefaults, "status", &esc.Status, EventStreamStatusStarted, func(v fftypes.FFEnum) bool { return fftypes.FFEnumValid(ctx, "esstatus", v) }); err != nil {
		return err
	}
	if err := checkSet(ctx, setDefaults, "batchSize", &esc.BatchSize, defaults.BatchSize, func(v int) bool { return v > 0 }); err != nil {
		return err
	}
	if err := checkSet(ctx, setDefaults, "batchTimeout", &esc.BatchTimeout, defaults.BatchTimeout, func(v fftypes.FFDuration) bool { return v > 0 }); err != nil {
		return err
	}
	if err := checkSet(ctx, setDefaults, "retryTimeout", &esc.RetryTimeout, defaults.RetryTimeout, func(v fftypes.FFDuration) bool { return v > 0 }); err != nil {
		return err
	}
	if err := checkSet(ctx, setDefaults, "blockedRetryDelay", &esc.BlockedRetryDelay, defaults.BlockedRetryDelay, func(v fftypes.FFDuration) bool { return v > 0 }); err != nil {
		return err
	}
	if err := checkSet(ctx, setDefaults, "errorHandling", &esc.ErrorHandling, defaults.ErrorHandling, func(v fftypes.FFEnum) bool { return fftypes.FFEnumValid(ctx, "ehtype", v) }); err != nil {
		return err
	}
	if err := checkSet(ctx, setDefaults, "type", &esc.Type, EventStreamTypeWebSocket, func(v fftypes.FFEnum) bool { return fftypes.FFEnumValid(ctx, "estype", v) }); err != nil {
		return err
	}
	switch *esc.Type {
	case EventStreamTypeWebSocket:
		if esc.WebSocket == nil {
			esc.WebSocket = &WebSocketConfig{}
		}
		if err := esc.WebSocket.Validate(ctx, &defaults.WebSocketDefaults, setDefaults); err != nil {
			return err
		}
	case EventStreamTypeWebhook:
		if esc.Webhook == nil {
			esc.Webhook = &WebhookConfig{}
		}
		if err := esc.Webhook.Validate(ctx, tlsConfigs); err != nil {
			return err
		}
	default:
		return i18n.NewError(ctx, i18n.MsgESInvalidType, *esc.Type)
	}
	return nil
}

type eventStream[CT any, DT any] struct {
	bgCtx       context.Context
	esm         *esManager[CT, DT]
	spec        *EventStreamSpec[CT]
	mux         sync.Mutex
	action      EventBatchDispatcher[DT]
	activeState *activeStream[CT, DT]
	retry       *retry.Retry
	persistence Persistence[CT]
	stopping    chan struct{}
}

type EventStreamActions[CT any] interface {
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
	Status(ctx context.Context) *EventStreamWithStatus[CT]
}

func (esm *esManager[CT, DT]) initEventStream(
	bgCtx context.Context,
	spec *EventStreamSpec[CT],
) (es *eventStream[CT, DT], err error) {
	// Validate
	if err := spec.Validate(bgCtx, esm.tlsConfigs, &esm.config.Defaults, true); err != nil {
		return nil, err
	}

	es = &eventStream[CT, DT]{
		bgCtx:       log.WithLogField(bgCtx, "eventstream", spec.ID.String()),
		esm:         esm,
		spec:        spec,
		persistence: esm.persistence,
		retry:       esm.config.Retry,
	}

	switch *es.spec.Type {
	case EventStreamTypeWebhook:
		if es.action, err = esm.newWebhookAction(es.bgCtx, spec.Webhook); err != nil {
			return nil, err
		}
	case EventStreamTypeWebSocket:
		es.action = newWebSocketAction[DT](esm.wsChannels, spec.WebSocket, *spec.Name)
	default:
		return nil, i18n.NewError(es.bgCtx, i18n.MsgESInvalidType, *es.spec.Type)
	}

	log.L(es.bgCtx).Infof("Initialized Event Stream")
	if *spec.Status == EventStreamStatusStarted {
		// Start up the stream
		es.ensureActive()
	}
	return es, nil
}

func (es *eventStream[CT, DT]) requestStop(ctx context.Context) chan struct{} {
	es.mux.Lock()
	defer es.mux.Unlock()
	// Check we're active
	activeState := es.activeState
	if activeState == nil {
		return nil
	}
	// Check if we're already stopping
	if es.stopping != nil {
		return es.stopping
	}
	persistedStatus := *es.spec.Status
	// Cancel the active context, and create the stopping task
	activeState.cancelCtx()
	es.stopping = make(chan struct{})
	go func() {
		<-activeState.eventLoopDone
		<-activeState.batchLoopDone

		// Complete an in-process delete
		if persistedStatus == EventStreamStatusDeleted {
			if err := es.persistStatus(ctx, persistedStatus); err != nil {
				log.L(ctx).Errorf("Failed to delete: %s", err)
			}
		}

		es.mux.Lock()
		defer es.mux.Unlock()
		es.activeState = nil
		close(es.stopping)
	}()
	return es.stopping
}

func (es *eventStream[CT, DT]) checkSetStatus(ctx context.Context, targetStatus *EventStreamStatus) (newRuntimeStatus EventStreamStatus, changeToPersist *EventStreamStatus, statistics *EventStreamStatistics, err error) {
	es.mux.Lock()
	defer es.mux.Unlock()

	transition := func(runtime, persited EventStreamStatus) {
		newRuntimeStatus = runtime
		es.spec.Status = &persited
		changeToPersist = &persited
	}

	// We snap a pointer to any stats under the lock here
	if es.activeState != nil {
		statistics = &es.activeState.EventStreamStatistics
	}

	// Check valid state transitions based on the persisted status, and whether we are stopping
	switch *es.spec.Status {
	case EventStreamStatusDeleted:
		newRuntimeStatus = EventStreamStatusDeleted
		if es.stopping != nil {
			newRuntimeStatus = EventStreamStatusStoppingDeleted
		}
		// No state changes possible from here
		if targetStatus != nil && *targetStatus != EventStreamStatusDeleted {
			err = i18n.NewError(ctx, i18n.MsgESDeleting)
		}
	case EventStreamStatusStopped:
		newRuntimeStatus = EventStreamStatusStopped
		if es.stopping != nil {
			newRuntimeStatus = EventStreamStatusStopping
		}
		// We can only stay in stopped, or go to deleted
		if targetStatus != nil {
			switch *targetStatus {
			case EventStreamStatusStopped:
				// no change
			case EventStreamStatusStarted:
				transition(EventStreamStatusStarted, EventStreamStatusStarted) // note no starting interim runtime status
			case EventStreamStatusDeleted:
				transition(EventStreamStatusStoppingDeleted, EventStreamStatusDeleted)
			default:
				err = i18n.NewError(ctx, i18n.MsgESStopping)
			}
		}
	case EventStreamStatusStarted:
		newRuntimeStatus = EventStreamStatusStarted
		// We can go anywhere
		if targetStatus != nil {
			switch *targetStatus {
			case EventStreamStatusStarted:
				// no change
			case EventStreamStatusStopped:
				transition(EventStreamStatusStopping, EventStreamStatusStopped)
			case EventStreamStatusDeleted:
				transition(EventStreamStatusStoppingDeleted, EventStreamStatusDeleted)
			}
		}
	default:
		err = i18n.NewError(ctx, i18n.MsgESInvalidPersistedStatus)
	}
	log.L(ctx).Infof("Status: %s (change=%v)", newRuntimeStatus, changeToPersist)
	return newRuntimeStatus, changeToPersist, statistics, err
}

func (es *eventStream[CT, DT]) persistStatus(ctx context.Context, targetStatus EventStreamStatus) error {
	fb := EventStreamFilters.NewUpdate(ctx)
	return es.esm.persistence.EventStreams().Update(ctx, es.spec.ID.String(), fb.Set("status", targetStatus))
}

func (es *eventStream[CT, DT]) stopOrDelete(ctx context.Context, targetStatus EventStreamStatus) error {
	_, newPersistedStatus, _, err := es.checkSetStatus(ctx, &targetStatus)
	if err != nil {
		return err
	}
	if newPersistedStatus != nil {
		if err := es.persistStatus(ctx, *newPersistedStatus); err != nil {
			return err
		}
	}

	// initiate a stop, if we're started
	stopping := es.requestStop(ctx)
	if stopping != nil {
		// wait for the stop to complete
		select {
		case <-stopping:
		case <-ctx.Done():
			return i18n.NewError(ctx, i18n.MsgESContextCancelledWaitingStop)
		}
	}
	return nil
}

func (es *eventStream[CT, DT]) ensureActive() {
	// Caller responsible for checking state transitions before invoking
	es.mux.Lock()
	defer es.mux.Unlock()
	if es.stopping != nil && es.activeState == nil {
		es.activeState = es.newActiveStream()
	}
}

func (es *eventStream[CT, DT]) Stop(ctx context.Context) error {
	return es.stopOrDelete(ctx, EventStreamStatusStopped)
}

func (es *eventStream[CT, DT]) Delete(ctx context.Context) error {
	return es.stopOrDelete(ctx, EventStreamStatusDeleted)
}

func (es *eventStream[CT, DT]) Start(ctx context.Context) error {
	startedStatus := EventStreamStatusStarted
	_, newPersistedStatus, _, err := es.checkSetStatus(ctx, &startedStatus)
	if err != nil {
		return err
	}
	if newPersistedStatus != nil {
		if err := es.persistStatus(ctx, *newPersistedStatus); err != nil {
			return err
		}
	}
	es.ensureActive()
	return nil
}

func (es *eventStream[CT, DT]) Status(ctx context.Context) *EventStreamWithStatus[CT] {
	status, _, statistics, _ := es.checkSetStatus(ctx, nil)
	return &EventStreamWithStatus[CT]{
		EventStreamSpec: es.spec,
		Status:          status,
		Statistics:      statistics,
	}
}
