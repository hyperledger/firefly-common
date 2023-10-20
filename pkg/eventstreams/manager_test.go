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
	"testing"

	"github.com/hyperledger/firefly-common/mocks/crudmocks"
	"github.com/hyperledger/firefly-common/pkg/config"
	"github.com/hyperledger/firefly-common/pkg/dbsql"
	"github.com/hyperledger/firefly-common/pkg/fftls"
	"github.com/hyperledger/firefly-common/pkg/retry"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

type mockEventSource struct {
	validate func(ctx context.Context, conf *testESConfig) error
	run      func(ctx context.Context, es *EventStreamSpec[testESConfig], checkpointSequenceId string, deliver Deliver[testData]) error
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
	events      *crudmocks.CRUD[*EventStreamSpec[testESConfig]]
	checkpoints *crudmocks.CRUD[*EventStreamCheckpoint]
}

func (mp *mockPersistence) EventStreams() dbsql.CRUD[*EventStreamSpec[testESConfig]] {
	return mp.events
}
func (mp *mockPersistence) Checkpoints() dbsql.CRUD[*EventStreamCheckpoint] {
	return mp.checkpoints
}
func (mp *mockPersistence) Close() {}

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

func newMockESManager(t *testing.T, extraSetup ...func(mp *mockPersistence)) (context.Context, *esManager[testESConfig, testData], *mockEventSource, func()) {
	logrus.SetLevel(logrus.DebugLevel)

	mp := &mockPersistence{
		events:      crudmocks.NewCRUD[*EventStreamSpec[testESConfig]](t),
		checkpoints: crudmocks.NewCRUD[*EventStreamCheckpoint](t),
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

	mes := &mockEventSource{}
	mgr, err := NewEventStreamManager[testESConfig, testData](ctx, GenerateConfig(ctx), mp, nil, mes)
	assert.NoError(t, err)

	return ctx, mgr.(*esManager[testESConfig, testData]), mes, func() {
		mgr.Close(ctx)
		cancelCtx()
	}
}

func ptrTo[T any](v T) *T {
	return &v
}
