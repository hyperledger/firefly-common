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
	"fmt"
	"testing"

	"github.com/hyperledger/firefly-common/mocks/crudmocks"
	"github.com/hyperledger/firefly-common/pkg/config"
	"github.com/hyperledger/firefly-common/pkg/dbsql"
	"github.com/hyperledger/firefly-common/pkg/ffapi"
	"github.com/hyperledger/firefly-common/pkg/fftls"
	"github.com/hyperledger/firefly-common/pkg/fftypes"
	"github.com/hyperledger/firefly-common/pkg/retry"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type mockEventSource struct {
	validate func(ctx context.Context, conf *testESConfig) error
	run      func(ctx context.Context, es *EventStreamSpec[testESConfig], checkpointSequenceId string, deliver Deliver[testData]) error
}

func (mes *mockEventSource) NewID() string {
	return fftypes.NewUUID().String()
}

func (mes *mockEventSource) Run(ctx context.Context, es *EventStreamSpec[testESConfig], checkpointSequenceId string, deliver Deliver[testData]) error {
	return mes.run(ctx, es, checkpointSequenceId, deliver)
}

func (mes *mockEventSource) Validate(ctx context.Context, conf *testESConfig) error {
	return mes.validate(ctx, conf)
}

type mockAction struct {
	attemptDispatch func(ctx context.Context, attempt int, events *EventBatch[testData]) error
}

func (ma *mockAction) AttemptDispatch(ctx context.Context, attempt int, events *EventBatch[testData]) error {
	return ma.attemptDispatch(ctx, attempt, events)
}

type mockPersistence struct {
	eventStreams *crudmocks.CRUD[*EventStreamSpec[testESConfig]]
	checkpoints  *crudmocks.CRUD[*EventStreamCheckpoint]
}

func (mp *mockPersistence) EventStreams() dbsql.CRUD[*EventStreamSpec[testESConfig]] {
	return mp.eventStreams
}
func (mp *mockPersistence) Checkpoints() dbsql.CRUD[*EventStreamCheckpoint] {
	return mp.checkpoints
}
func (mp *mockPersistence) Close() {}

func newMockESManager(t *testing.T, extraSetup ...func(mp *mockPersistence)) (context.Context, *esManager[testESConfig, testData], *mockEventSource, func()) {
	logrus.SetLevel(logrus.DebugLevel)

	mp := &mockPersistence{
		eventStreams: crudmocks.NewCRUD[*EventStreamSpec[testESConfig]](t),
		checkpoints:  crudmocks.NewCRUD[*EventStreamCheckpoint](t),
	}

	ctx, cancelCtx := context.WithCancel(context.Background())
	config.RootConfigReset()
	conf := config.RootSection("ut")
	dbConf := conf.SubSection("db")
	esConf := conf.SubSection("eventstreams")
	dbsql.InitSQLiteConfig(dbConf)
	InitConfig(esConf)

	CheckpointsConfig.Set(ConfigCheckpointsAsynchronous, false)
	for _, fn := range extraSetup {
		fn(mp)
	}

	mes := &mockEventSource{
		validate: func(ctx context.Context, conf *testESConfig) error { return nil },
		run: func(ctx context.Context, es *EventStreamSpec[testESConfig], checkpointSequenceId string, deliver Deliver[testData]) error {
			<-ctx.Done()
			return nil
		},
	}
	mgr, err := NewEventStreamManager[testESConfig, testData](ctx, GenerateConfig(ctx), mp, nil, mes)
	assert.NoError(t, err)

	return ctx, mgr.(*esManager[testESConfig, testData]), mes, func() {
		mgr.Close(ctx)
		cancelCtx()
	}
}

func TestNewManagerFailBadTLS(t *testing.T) {
	_, err := NewEventStreamManager[testESConfig, testData](context.Background(), &Config{
		Retry: &retry.Retry{},
		TLSConfigs: map[string]*fftls.Config{
			"tls0": {
				Enabled: true,
				CAFile:  t.TempDir(),
			},
		},
	}, nil, nil, &mockEventSource{})
	assert.Regexp(t, "FF00153", err)

}

func TestNewManagerBadConfStruct(t *testing.T) {
	assert.Panics(t, func() {
		_, _ = NewEventStreamManager[string /* must be DBSerializable */, testData](context.Background(), &Config{
			Retry: &retry.Retry{},
			TLSConfigs: map[string]*fftls.Config{
				"tls0": {
					Enabled: true,
					CAFile:  t.TempDir(),
				},
			},
		}, nil, nil, nil)
	})
}

func TestNewManagerBadConfState(t *testing.T) {
	_, err := NewEventStreamManager[testESConfig, testData](context.Background(), &Config{}, nil, nil, &mockEventSource{})
	assert.Regexp(t, "FF00237", err)
}

func TestInitFail(t *testing.T) {
	mp := &mockPersistence{
		eventStreams: crudmocks.NewCRUD[*EventStreamSpec[testESConfig]](t),
		checkpoints:  crudmocks.NewCRUD[*EventStreamCheckpoint](t),
	}
	mp.eventStreams.On("GetMany", mock.Anything, mock.Anything).Return([]*EventStreamSpec[testESConfig]{}, &ffapi.FilterResult{}, fmt.Errorf("pop"))
	ctx := context.Background()
	InitConfig(config.RootSection("ut"))
	_, err := NewEventStreamManager[testESConfig, testData](ctx, GenerateConfig(ctx), mp, nil, nil)
	assert.Regexp(t, "pop", err)
}

func TestInitWithStreams(t *testing.T) {
	es := &EventStreamSpec[testESConfig]{
		ID:     ptrTo(fftypes.NewUUID().String()),
		Name:   ptrTo("stream1"),
		Status: ptrTo(EventStreamStatusStarted),
	}
	_, _, _, done := newMockESManager(t, func(mp *mockPersistence) {
		mp.eventStreams.On("GetMany", mock.Anything, mock.Anything).Return([]*EventStreamSpec[testESConfig]{es}, &ffapi.FilterResult{}, nil).Once()
		mp.eventStreams.On("GetMany", mock.Anything, mock.Anything).Return([]*EventStreamSpec[testESConfig]{}, &ffapi.FilterResult{}, nil).Once()
		mp.checkpoints.On("GetByID", mock.Anything, es.GetID()).Return((*EventStreamCheckpoint)(nil), nil)
	})
	defer done()

}

func TestInitWithStreamsCleanupFail(t *testing.T) {
	mp := &mockPersistence{
		eventStreams: crudmocks.NewCRUD[*EventStreamSpec[testESConfig]](t),
		checkpoints:  crudmocks.NewCRUD[*EventStreamCheckpoint](t),
	}
	ctx := context.Background()
	InitConfig(config.RootSection("ut"))
	es := &EventStreamSpec[testESConfig]{
		ID:     ptrTo(fftypes.NewUUID().String()),
		Name:   ptrTo("stream1"),
		Status: ptrTo(EventStreamStatusDeleted),
	}
	mp.eventStreams.On("GetMany", mock.Anything, mock.Anything).Return([]*EventStreamSpec[testESConfig]{es}, &ffapi.FilterResult{}, nil).Once()
	mp.eventStreams.On("Delete", mock.Anything, es.GetID()).Return(fmt.Errorf("pop"))
	_, err := NewEventStreamManager[testESConfig, testData](ctx, GenerateConfig(ctx), mp, nil, nil)
	assert.Regexp(t, "pop", err)
}

func TestInitWithStreamsInitFail(t *testing.T) {
	mp := &mockPersistence{
		eventStreams: crudmocks.NewCRUD[*EventStreamSpec[testESConfig]](t),
		checkpoints:  crudmocks.NewCRUD[*EventStreamCheckpoint](t),
	}
	ctx := context.Background()
	InitConfig(config.RootSection("ut"))
	es := &EventStreamSpec[testESConfig]{
		ID:     ptrTo(fftypes.NewUUID().String()),
		Name:   ptrTo("stream1"),
		Status: ptrTo(EventStreamStatusStarted),
	}
	mp.eventStreams.On("GetMany", mock.Anything, mock.Anything).Return([]*EventStreamSpec[testESConfig]{es}, &ffapi.FilterResult{}, nil).Once()
	_, err := NewEventStreamManager[testESConfig, testData](ctx, GenerateConfig(ctx), mp, nil, &mockEventSource{
		validate: func(ctx context.Context, conf *testESConfig) error {
			return fmt.Errorf("pop")
		},
	})
	assert.Regexp(t, "pop", err)
}

func TestUpsertStreamDeleted(t *testing.T) {
	es := &EventStreamSpec[testESConfig]{
		ID:     ptrTo(fftypes.NewUUID().String()),
		Name:   ptrTo("stream1"),
		Status: ptrTo(EventStreamStatusStopped),
	}
	ctx, esm, _, done := newMockESManager(t, func(mp *mockPersistence) {
		mp.eventStreams.On("GetMany", mock.Anything, mock.Anything).Return([]*EventStreamSpec[testESConfig]{es}, &ffapi.FilterResult{}, nil).Once()
		mp.eventStreams.On("GetMany", mock.Anything, mock.Anything).Return([]*EventStreamSpec[testESConfig]{}, &ffapi.FilterResult{}, nil).Once()
	})
	defer done()

	esm.getStream(es.GetID()).spec.Status = ptrTo(EventStreamStatusDeleted)
	_, err := esm.UpsertStream(ctx, es)
	assert.Regexp(t, "FF00236", err)

}

func TestUpsertStreamBadUpdate(t *testing.T) {
	es := &EventStreamSpec[testESConfig]{
		ID:     ptrTo(fftypes.NewUUID().String()),
		Name:   ptrTo("stream1"),
		Status: ptrTo(EventStreamStatusStopped),
	}
	ctx, esm, _, done := newMockESManager(t, func(mp *mockPersistence) {
		mp.eventStreams.On("GetMany", mock.Anything, mock.Anything).Return([]*EventStreamSpec[testESConfig]{es}, &ffapi.FilterResult{}, nil).Once()
		mp.eventStreams.On("GetMany", mock.Anything, mock.Anything).Return([]*EventStreamSpec[testESConfig]{}, &ffapi.FilterResult{}, nil).Once()
	})
	defer done()

	newES := *es
	newES.Name = nil
	_, err := esm.UpsertStream(ctx, &newES)
	assert.Regexp(t, "FF00112", err)

}

func TestUpsertStreamUpsertFail(t *testing.T) {
	es := &EventStreamSpec[testESConfig]{
		ID:     ptrTo(fftypes.NewUUID().String()),
		Name:   ptrTo("stream1"),
		Status: ptrTo(EventStreamStatusStopped),
	}
	ctx, esm, _, done := newMockESManager(t, func(mp *mockPersistence) {
		mp.eventStreams.On("GetMany", mock.Anything, mock.Anything).Return([]*EventStreamSpec[testESConfig]{es}, &ffapi.FilterResult{}, nil).Once()
		mp.eventStreams.On("GetMany", mock.Anything, mock.Anything).Return([]*EventStreamSpec[testESConfig]{}, &ffapi.FilterResult{}, nil).Once()
		mp.eventStreams.On("Upsert", mock.Anything, mock.Anything, dbsql.UpsertOptimizationExisting).Return(false, fmt.Errorf("pop")).Once()
	})
	defer done()

	_, err := esm.UpsertStream(ctx, es)
	assert.Regexp(t, "pop", err)

}

func TestUpsertReInitExistingFailTimeout(t *testing.T) {
	es := &EventStreamSpec[testESConfig]{
		ID:     ptrTo(fftypes.NewUUID().String()),
		Name:   ptrTo("stream1"),
		Status: ptrTo(EventStreamStatusStopped),
	}
	ctx, esm, _, done := newMockESManager(t, func(mp *mockPersistence) {
		mp.eventStreams.On("GetMany", mock.Anything, mock.Anything).Return([]*EventStreamSpec[testESConfig]{}, &ffapi.FilterResult{}, nil).Once()
	})
	done()

	existing := &eventStream[testESConfig, testData]{
		activeState: &activeStream[testESConfig, testData]{},
		stopping:    make(chan struct{}),
	}
	err := esm.reInit(ctx, es, existing)
	assert.Regexp(t, "FF00229", err)

}

func TestUpsertReInitExistingFailInit(t *testing.T) {
	es := &EventStreamSpec[testESConfig]{
		ID:     ptrTo(fftypes.NewUUID().String()),
		Name:   ptrTo("stream1"),
		Status: ptrTo(fftypes.FFEnum("wrong")),
	}
	ctx, esm, _, done := newMockESManager(t, func(mp *mockPersistence) {
		mp.eventStreams.On("GetMany", mock.Anything, mock.Anything).Return([]*EventStreamSpec[testESConfig]{}, &ffapi.FilterResult{}, nil).Once()
	})
	done()

	err := esm.reInit(ctx, es, nil)
	assert.Regexp(t, "FF00234", err)

}

func TestDeleteStreamNotKnown(t *testing.T) {
	ctx, esm, _, done := newMockESManager(t, func(mp *mockPersistence) {
		mp.eventStreams.On("GetMany", mock.Anything, mock.Anything).Return([]*EventStreamSpec[testESConfig]{}, &ffapi.FilterResult{}, nil).Once()
	})
	defer done()

	err := esm.DeleteStream(ctx, fftypes.NewUUID().String())
	assert.Regexp(t, "FF00164", err)

}

func TestResetStreamNotKnown(t *testing.T) {
	ctx, esm, _, done := newMockESManager(t, func(mp *mockPersistence) {
		mp.eventStreams.On("GetMany", mock.Anything, mock.Anything).Return([]*EventStreamSpec[testESConfig]{}, &ffapi.FilterResult{}, nil).Once()
	})
	defer done()

	err := esm.ResetStream(ctx, fftypes.NewUUID().String(), "")
	assert.Regexp(t, "FF00164", err)

}

func TestStopStreamNotKnown(t *testing.T) {
	ctx, esm, _, done := newMockESManager(t, func(mp *mockPersistence) {
		mp.eventStreams.On("GetMany", mock.Anything, mock.Anything).Return([]*EventStreamSpec[testESConfig]{}, &ffapi.FilterResult{}, nil).Once()
	})
	defer done()

	err := esm.StopStream(ctx, fftypes.NewUUID().String())
	assert.Regexp(t, "FF00164", err)

}

func TestStartStreamNotKnown(t *testing.T) {
	ctx, esm, _, done := newMockESManager(t, func(mp *mockPersistence) {
		mp.eventStreams.On("GetMany", mock.Anything, mock.Anything).Return([]*EventStreamSpec[testESConfig]{}, &ffapi.FilterResult{}, nil).Once()
	})
	defer done()

	err := esm.StartStream(ctx, fftypes.NewUUID().String())
	assert.Regexp(t, "FF00164", err)

}

func TestEnrichStreamNotKnown(t *testing.T) {
	ctx, esm, _, done := newMockESManager(t, func(mp *mockPersistence) {
		mp.eventStreams.On("GetMany", mock.Anything, mock.Anything).Return([]*EventStreamSpec[testESConfig]{}, &ffapi.FilterResult{}, nil).Once()
	})
	defer done()

	es := esm.enrichGetStream(ctx, &EventStreamSpec[testESConfig]{
		ID: ptrTo(fftypes.NewUUID().String()),
	})
	assert.NotNil(t, es)
	assert.Equal(t, EventStreamStatusUnknown, es.Status)

}

func TestDeleteStreamFail(t *testing.T) {
	es := &EventStreamSpec[testESConfig]{
		ID:     ptrTo(fftypes.NewUUID().String()),
		Name:   ptrTo("stream1"),
		Status: ptrTo(EventStreamStatusStopped),
	}
	ctx, esm, _, done := newMockESManager(t, func(mp *mockPersistence) {
		mp.eventStreams.On("GetMany", mock.Anything, mock.Anything).Return([]*EventStreamSpec[testESConfig]{es}, &ffapi.FilterResult{}, nil).Once()
		mp.eventStreams.On("GetMany", mock.Anything, mock.Anything).Return([]*EventStreamSpec[testESConfig]{}, &ffapi.FilterResult{}, nil).Once()
		mp.eventStreams.On("Update", mock.Anything, mock.Anything, mock.Anything).Return(fmt.Errorf("pop")).Once()
	})
	defer done()

	err := esm.DeleteStream(ctx, es.GetID())
	assert.Regexp(t, "pop", err)

}

func TestDeleteStreamFailDelete(t *testing.T) {
	es := &EventStreamSpec[testESConfig]{
		ID:     ptrTo(fftypes.NewUUID().String()),
		Name:   ptrTo("stream1"),
		Status: ptrTo(EventStreamStatusStopped),
	}
	ctx, esm, _, done := newMockESManager(t, func(mp *mockPersistence) {
		mp.eventStreams.On("GetMany", mock.Anything, mock.Anything).Return([]*EventStreamSpec[testESConfig]{es}, &ffapi.FilterResult{}, nil).Once()
		mp.eventStreams.On("GetMany", mock.Anything, mock.Anything).Return([]*EventStreamSpec[testESConfig]{}, &ffapi.FilterResult{}, nil).Once()
		mp.eventStreams.On("Update", mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
		mp.eventStreams.On("Delete", mock.Anything, mock.Anything).Return(fmt.Errorf("pop")).Once()
	})
	defer done()

	err := esm.DeleteStream(ctx, es.GetID())
	assert.Regexp(t, "pop", err)

}

func TestResetStreamStopFailTimeout(t *testing.T) {
	ctx, esm, _, done := newMockESManager(t, func(mp *mockPersistence) {
		mp.eventStreams.On("GetMany", mock.Anything, mock.Anything).Return([]*EventStreamSpec[testESConfig]{}, &ffapi.FilterResult{}, nil).Once()
	})
	done()

	existing := &eventStream[testESConfig, testData]{
		activeState: &activeStream[testESConfig, testData]{},
		stopping:    make(chan struct{}),
		spec: &EventStreamSpec[testESConfig]{
			ID:     ptrTo(fftypes.NewUUID().String()),
			Name:   ptrTo("stream1"),
			Status: ptrTo(EventStreamStatusStopped),
		},
	}
	esm.addStream(ctx, existing)
	err := esm.ResetStream(ctx, existing.spec.GetID(), "")
	assert.Regexp(t, "FF00229", err)

}

func TestResetStreamStopFailDeleteCheckpoint(t *testing.T) {
	ctx, esm, _, done := newMockESManager(t, func(mp *mockPersistence) {
		mp.eventStreams.On("GetMany", mock.Anything, mock.Anything).Return([]*EventStreamSpec[testESConfig]{}, &ffapi.FilterResult{}, nil).Once()
		mp.checkpoints.On("DeleteMany", mock.Anything, mock.Anything).Return(fmt.Errorf("pop")).Once()
	})
	done()

	existing := &eventStream[testESConfig, testData]{
		spec: &EventStreamSpec[testESConfig]{
			ID:     ptrTo(fftypes.NewUUID().String()),
			Name:   ptrTo("stream1"),
			Status: ptrTo(EventStreamStatusStopped),
		},
	}
	esm.addStream(ctx, existing)
	err := esm.ResetStream(ctx, existing.spec.GetID(), "")
	assert.Regexp(t, "pop", err)

}

func TestResetStreamStopFailUpdateSequence(t *testing.T) {
	ctx, esm, _, done := newMockESManager(t, func(mp *mockPersistence) {
		mp.eventStreams.On("GetMany", mock.Anything, mock.Anything).Return([]*EventStreamSpec[testESConfig]{}, &ffapi.FilterResult{}, nil).Once()
		mp.checkpoints.On("DeleteMany", mock.Anything, mock.Anything).Return(nil).Once()
		mp.eventStreams.On("UpdateSparse", mock.Anything, mock.Anything).Return(fmt.Errorf("pop")).Once()
	})
	done()

	existing := &eventStream[testESConfig, testData]{
		spec: &EventStreamSpec[testESConfig]{
			ID:     ptrTo(fftypes.NewUUID().String()),
			Name:   ptrTo("stream1"),
			Status: ptrTo(EventStreamStatusStopped),
		},
	}
	esm.addStream(ctx, existing)
	err := esm.ResetStream(ctx, existing.spec.GetID(), "12345")
	assert.Regexp(t, "pop", err)

}

func TestResetStreamNoOp(t *testing.T) {
	ctx, esm, _, done := newMockESManager(t, func(mp *mockPersistence) {
		mp.eventStreams.On("GetMany", mock.Anything, mock.Anything).Return([]*EventStreamSpec[testESConfig]{}, &ffapi.FilterResult{}, nil).Once()
		mp.checkpoints.On("DeleteMany", mock.Anything, mock.Anything).Return(nil).Once()
		mp.eventStreams.On("UpdateSparse", mock.Anything, mock.Anything).Return(nil).Once()
	})
	done()

	existing := &eventStream[testESConfig, testData]{
		spec: &EventStreamSpec[testESConfig]{
			ID:     ptrTo(fftypes.NewUUID().String()),
			Name:   ptrTo("stream1"),
			Status: ptrTo(EventStreamStatusStopped),
		},
	}
	esm.addStream(ctx, existing)
	err := esm.ResetStream(ctx, existing.spec.GetID(), "12345")
	assert.NoError(t, err)

}

func TestListStreamsFail(t *testing.T) {
	ctx, esm, _, done := newMockESManager(t, func(mp *mockPersistence) {
		mp.eventStreams.On("GetMany", mock.Anything, mock.Anything).Return([]*EventStreamSpec[testESConfig]{}, &ffapi.FilterResult{}, nil).Once()
		mp.eventStreams.On("GetMany", mock.Anything, mock.Anything).Return([]*EventStreamSpec[testESConfig]{}, &ffapi.FilterResult{}, fmt.Errorf("pop")).Once()
	})
	defer done()

	_, _, err := esm.ListStreams(ctx, EventStreamFilters.NewFilter(ctx).And())
	assert.Regexp(t, "pop", err)

}

func TestGetStreamByIDFail(t *testing.T) {
	ctx, esm, _, done := newMockESManager(t, func(mp *mockPersistence) {
		mp.eventStreams.On("GetMany", mock.Anything, mock.Anything).Return([]*EventStreamSpec[testESConfig]{}, &ffapi.FilterResult{}, nil).Once()
		mp.eventStreams.On("GetByID", mock.Anything, mock.Anything).Return((*EventStreamSpec[testESConfig])(nil), fmt.Errorf("pop")).Once()
	})
	defer done()

	_, err := esm.GetStreamByID(ctx, fftypes.NewUUID().String())
	assert.Regexp(t, "pop", err)

}

func TestCloseSuspendFail(t *testing.T) {
	ctx, esm, _, done := newMockESManager(t, func(mp *mockPersistence) {
		mp.eventStreams.On("GetMany", mock.Anything, mock.Anything).Return([]*EventStreamSpec[testESConfig]{}, &ffapi.FilterResult{}, nil).Once()
	})
	done()

	existing := &eventStream[testESConfig, testData]{
		spec: &EventStreamSpec[testESConfig]{
			ID:     ptrTo(fftypes.NewUUID().String()),
			Name:   ptrTo("stream1"),
			Status: ptrTo(EventStreamStatusStopped),
		},
		activeState: &activeStream[testESConfig, testData]{},
		stopping:    make(chan struct{}),
	}
	esm.addStream(ctx, existing)
	esm.Close(ctx)

}
