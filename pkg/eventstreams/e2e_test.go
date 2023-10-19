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
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/hyperledger/firefly-common/pkg/config"
	"github.com/hyperledger/firefly-common/pkg/dbsql"
	"github.com/hyperledger/firefly-common/pkg/wsserver"
	"github.com/stretchr/testify/assert"
)

type testESConfig struct {
	Config1 string `json:"config1"`
}

type testData struct {
	Field1 string `json:"field1"`
}

type testSource struct{}

func (ts *testSource) Validate(ctx context.Context, config *testESConfig) error {
	if config.Config1 == "" {
		return fmt.Errorf("config1 missing")
	}
	return nil
}

func (ts *testSource) Run(ctx context.Context, spec *EventStreamSpec[testESConfig], checkpointSequenceID string, deliver Deliver) error {
	i := 0
	for {
		events := make([]*Event, 10)
		for i2 := 0; i2 < 10; i2++ {
			topic := fmt.Sprintf("topic_%d", i2)
			events[i] = &Event{
				Topic:      topic,
				SequenceID: fmt.Sprintf("%.12d", i),
				Data: &testData{
					Field1: fmt.Sprintf("data_%d", i),
				},
			}
		}
		if deliver(events) == Exit {
			return nil
		}
		i++
	}
}

// This test demonstrates the function of the evenstream module through a simple test,
// using an SQLite database for CRUD operations on the persisted event streams,
// and a fake stream of events.
func TestE2E(t *testing.T) {

	ctx := context.Background()
	config.RootConfigReset()
	conf := config.RootSection("ut")
	dbConf := conf.SubSection("db")
	esConf := conf.SubSection("eventstreams")
	dbsql.InitSQLiteConfig(dbConf)
	InitConfig(esConf)

	dbConf.Set(dbsql.SQLConfMigrationsAuto, true)
	dbConf.Set(dbsql.SQLConfDatasourceURL, "file::memory:")
	dbConf.Set(dbsql.SQLConfMigrationsAuto, true)
	dbConf.Set(dbsql.SQLConfMigrationsDirectory, "../../test/es_demo_migrations")
	dbConf.Set(dbsql.SQLConfMaxConnections, 1)

	db, err := dbsql.NewSQLiteProvider(ctx, dbConf)
	assert.NoError(t, err)

	testWSServer := wsserver.NewWebSocketServer(ctx)
	server := httptest.NewServer(http.HandlerFunc(testWSServer.Handler))
	defer server.Close()

	ts := &testSource{}

	p := NewEventStreamPersistence[testESConfig](db)
	mgr, err := NewEventStreamManager[testESConfig](ctx, GenerateConfig(ctx), p, testWSServer, ts)
	assert.NoError(t, err)

	// Create an event stream
	created, err := mgr.UpsertStream(ctx, &EventStreamSpec[testESConfig]{
		Name:        strptr("stream1"),
		TopicFilter: strptr("topic3"), // only one of the topics
		Type:        &EventStreamTypeWebSocket,
	})
	assert.True(t, created)
	assert.NoError(t, err)

}
