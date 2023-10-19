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
	"fmt"
	"sync"

	"github.com/hyperledger/firefly-common/pkg/dbsql"
	"github.com/hyperledger/firefly-common/pkg/ffapi"
	"github.com/hyperledger/firefly-common/pkg/fftls"
	"github.com/hyperledger/firefly-common/pkg/fftypes"
	"github.com/hyperledger/firefly-common/pkg/i18n"
	"github.com/hyperledger/firefly-common/pkg/log"
	"github.com/hyperledger/firefly-common/pkg/wsserver"
)

type Manager[CT any] interface {
	UpsertStream(ctx context.Context, esSpec *EventStreamSpec[CT]) (bool, error)
	GetStreamByID(ctx context.Context, id *fftypes.UUID, opts ...dbsql.GetOption) (*EventStreamWithStatus[CT], error)
	ListStreams(ctx context.Context, filter ffapi.Filter) ([]*EventStreamWithStatus[CT], *ffapi.FilterResult, error)
	StopStream(ctx context.Context, id *fftypes.UUID) error
	StartStream(ctx context.Context, id *fftypes.UUID) error
	DeleteStream(ctx context.Context, id *fftypes.UUID) error
	Close(ctx context.Context)
}

type SourceInstruction int

const (
	Continue SourceInstruction = iota
	Exit
)

type Deliver[DT any] func(events []*Event[DT]) SourceInstruction

// Runtime is the required implementation extension for the EventStream common utility
type Runtime[ConfigType any, DataType any] interface {
	// Type specific config validation goes here
	Validate(ctx context.Context, config *ConfigType) error
	// The run function should execute in a loop detecting events until instructed to stop:
	// - The Run function should block when no events are available
	//   - Must detect if the context is closed (see below)
	// - The Deliver function will block if the stream is blocked:
	//   - Blocked means the previous batch is being processed, and the current batch is full
	// - If the stream stops, the Exit instruction will be returned from deliver
	// - The supplied context will be cancelled as well on exit, so should be used:
	//   1. In any blocking i/o functions
	//   2. To wake any sleeps early, such as batch polling scenarios
	// - If the function returns without an Exit instruction, it will be restarted from the last checkpoint
	Run(ctx context.Context, spec *EventStreamSpec[ConfigType], checkpointSequenceID string, deliver Deliver[DataType]) error
}

type esManager[CT any, DT any] struct {
	config      Config
	mux         sync.Mutex
	streams     map[fftypes.UUID]*eventStream[CT, DT]
	tlsConfigs  map[string]*tls.Config
	wsChannels  wsserver.WebSocketChannels
	persistence Persistence[CT]
	runtime     Runtime[CT, DT]
}

func NewEventStreamManager[CT any, DT any](ctx context.Context, config *Config, p Persistence[CT], wsChannels wsserver.WebSocketChannels, source Runtime[CT, DT]) (es Manager[CT], err error) {

	var confExample interface{} = new(CT)
	if _, isDBSerializable := (confExample).(DBSerializable); !isDBSerializable {
		panic(fmt.Sprintf("Config type must be DB serializable: %T", confExample))
	}

	// Parse the TLS configs up front
	tlsConfigs := make(map[string]*tls.Config)
	for name, tlsJSONConf := range config.TLSConfigs {
		tlsConfigs[name], err = fftls.NewTLSConfig(ctx, tlsJSONConf, fftls.ClientType)
		if err != nil {
			return nil, err
		}
	}
	return &esManager[CT, DT]{
		config:      *config,
		tlsConfigs:  tlsConfigs,
		runtime:     source,
		persistence: p,
		wsChannels:  wsChannels,
		streams:     map[fftypes.UUID]*eventStream[CT, DT]{},
	}, nil
}

func (esm *esManager[CT, DT]) addStream(ctx context.Context, es *eventStream[CT, DT]) {
	log.L(ctx).Infof("Adding stream '%s' [%s] (%s)", *es.spec.Name, es.spec.ID, es.Status(ctx).Status)
	esm.mux.Lock()
	defer esm.mux.Unlock()
	esm.streams[*es.spec.ID] = es
}

func (esm *esManager[CT, DT]) getStream(id *fftypes.UUID) *eventStream[CT, DT] {
	esm.mux.Lock()
	defer esm.mux.Unlock()
	return esm.streams[*id]
}

func (esm *esManager[CT, DT]) removeStream(id *fftypes.UUID) {
	esm.mux.Lock()
	defer esm.mux.Unlock()
	delete(esm.streams, *id)
}

func (esm *esManager[CT, DT]) Initialize(ctx context.Context) error {
	const pageSize = 25
	var skip uint64
	for {
		fb := EventStreamFilters.NewFilter(ctx)
		streams, _, err := esm.persistence.EventStreams().GetMany(ctx, fb.And().Skip(skip).Limit(pageSize))
		if err != nil {
			return err
		}
		if len(streams) == 0 {
			break
		}
		for _, esSpec := range streams {
			if *esSpec.Status == EventStreamStatusDeleted {
				if err := esm.persistence.EventStreams().Delete(ctx, esSpec.ID.String()); err != nil {
					return err
				}
			} else {
				es, err := esm.initEventStream(ctx, esSpec)
				if err != nil {
					return err
				}
				esm.addStream(ctx, es)
			}
		}
		skip += pageSize
	}
	return nil
}

func (esm *esManager[CT, DT]) UpsertStream(ctx context.Context, esSpec *EventStreamSpec[CT]) (bool, error) {
	if esSpec.ID == nil {
		esSpec.ID = fftypes.NewUUID()
	}

	// Only statuses that can be asserted externally are started/stopped
	if esSpec.Status == nil {
		esSpec.Status = &EventStreamStatusStarted
	}
	if *esSpec.Status != EventStreamStatusStarted && *esSpec.Status != EventStreamStatusStopped {
		return false, i18n.NewError(ctx, i18n.MsgESStartedOrStopped)
	}

	// Do a validation that does NOT update the defaults into the structure, so that
	// the defaults are not persisted into the DB. This means that if the defaults are
	// changed then any streams with nil fields will pick up the new defaults.
	if err := esSpec.Validate(ctx, esm.tlsConfigs, &esm.config.Defaults, false); err != nil {
		return false, err
	}

	isNew, err := esm.persistence.EventStreams().Upsert(ctx, esSpec, dbsql.UpsertOptimizationExisting)
	if err != nil {
		return false, err
	}

	es, err := esm.initEventStream(ctx, esSpec)
	if err != nil {
		return false, err
	}
	esm.addStream(ctx, es)
	return isNew, nil
}

func (esm *esManager[CT, DT]) DeleteStream(ctx context.Context, id *fftypes.UUID) error {
	es := esm.getStream(id)
	if es == nil {
		return i18n.NewError(ctx, i18n.Msg404NoResult)
	}
	if err := es.Delete(ctx); err != nil {
		return err
	}
	// Now we can delete it fully from the DB
	if err := esm.persistence.EventStreams().Delete(ctx, id.String()); err != nil {
		return err
	}
	esm.removeStream(id)
	return nil
}

func (esm *esManager[CT, DT]) StopStream(ctx context.Context, id *fftypes.UUID) error {
	es := esm.getStream(id)
	if es == nil {
		return i18n.NewError(ctx, i18n.Msg404NoResult)
	}
	return es.Stop(ctx)
}

func (esm *esManager[CT, DT]) StartStream(ctx context.Context, id *fftypes.UUID) error {
	es := esm.getStream(id)
	if es == nil {
		return i18n.NewError(ctx, i18n.Msg404NoResult)
	}
	return es.Start(ctx)
}

func (esm *esManager[CT, DT]) enrichGetStream(ctx context.Context, esSpec *EventStreamSpec[CT]) *EventStreamWithStatus[CT] {
	// Grab the live status
	if es := esm.getStream(esSpec.ID); es != nil {
		return es.Status(ctx)
	}
	// Fallback to unknown status rather than failing
	log.L(ctx).Errorf("No in-memory state for stream '%s'", esSpec.ID)
	return &EventStreamWithStatus[CT]{
		EventStreamSpec: esSpec,
		Status:          EventStreamStatusUnknown,
	}
}

func (esm *esManager[CT, DT]) ListStreams(ctx context.Context, filter ffapi.Filter) ([]*EventStreamWithStatus[CT], *ffapi.FilterResult, error) {
	results, fr, err := esm.persistence.EventStreams().GetMany(ctx, filter)
	if err != nil {
		return nil, nil, err
	}
	// Fill in live status and defaults
	enriched := make([]*EventStreamWithStatus[CT], len(results))
	for i, esSpec := range results {
		enriched[i] = esm.enrichGetStream(ctx, esSpec)
	}
	return enriched, fr, err
}

func (esm *esManager[CT, DT]) GetStreamByID(ctx context.Context, id *fftypes.UUID, opts ...dbsql.GetOption) (*EventStreamWithStatus[CT], error) {
	esSpec, err := esm.persistence.EventStreams().GetByID(ctx, id.String(), opts...)
	if err != nil {
		return nil, err
	}
	return esm.enrichGetStream(ctx, esSpec), nil
}

func (esm *esManager[CT, DT]) Close(ctx context.Context) {
	for _, es := range esm.streams {
		if err := es.Stop(ctx); err != nil {
			log.L(ctx).Warnf("Failed to stop event stream %s: %s", es.spec.ID, err)
		}
	}
}
