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
	"database/sql/driver"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/hyperledger/firefly-common/pkg/config"
	"github.com/hyperledger/firefly-common/pkg/dbsql"
	"github.com/hyperledger/firefly-common/pkg/fftypes"
	"github.com/hyperledger/firefly-common/pkg/wsclient"
	"github.com/hyperledger/firefly-common/pkg/wsserver"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

type testESConfig struct {
	Config1 string `json:"config1"`
}

func (tc *testESConfig) Scan(src interface{}) error {
	return fftypes.JSONScan(src, tc)
}

func (tc *testESConfig) Value() (driver.Value, error) {
	return fftypes.JSONValue(tc)
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

func (ts *testSource) Run(ctx context.Context, spec *EventStreamSpec[testESConfig], checkpointSequenceID string, deliver Deliver[testData]) error {
	i := 0
	for {
		events := make([]*Event[testData], 10)
		for i2 := 0; i2 < 10; i2++ {
			topic := fmt.Sprintf("topic_%d", i2)
			events[i] = &Event[testData]{
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

// This test demonstrates the runtime function of the event stream module through a simple test,
// using an SQLite database for CRUD operations on the persisted event streams,
// and a fake stream of events.

// This test demonstrates the CRUD features
func TestE2E_CRUDLifecycle(t *testing.T) {
	ctx, p, wss, _, done := setupE2ETest(t)
	defer done()

	ts := &testSource{}

	mgr, err := NewEventStreamManager[testESConfig, testData](ctx, GenerateConfig(ctx), p, wss, ts)
	assert.NoError(t, err)

	// Create first event stream started
	es1 := &EventStreamSpec[testESConfig]{
		Name:        strptr("stream1"),
		TopicFilter: strptr("topic1"), // only one of the topics
		Type:        &EventStreamTypeWebSocket,
		Config: &testESConfig{
			Config1: "confValue1",
		},
	}
	created, err := mgr.UpsertStream(ctx, es1)
	assert.NoError(t, err)
	assert.True(t, created)

	// Create second event stream stopped
	es2 := &EventStreamSpec[testESConfig]{
		Name:        strptr("stream2"),
		TopicFilter: strptr("topic2"), // only one of the topics
		Type:        &EventStreamTypeWebSocket,
		Status:      &EventStreamStatusStopped,
		Config: &testESConfig{
			Config1: "confValue2",
		},
	}
	created, err = mgr.UpsertStream(ctx, es2)
	assert.NoError(t, err)
	assert.True(t, created)

	// Find the second one by topic filter
	esList, _, err := mgr.ListStreams(ctx, EventStreamFilters.NewFilter(ctx).Eq("topicfilter", "topic2"))
	assert.NoError(t, err)
	assert.Len(t, esList, 1)
	assert.Equal(t, "stream2", *esList[0].Name)
	assert.Equal(t, "topic2", *esList[0].TopicFilter)
	assert.Equal(t, 50, *esList[0].BatchSize) // picked up default when it was loaded
	assert.Equal(t, "confValue2", esList[0].Config.Config1)
	assert.Equal(t, EventStreamStatusStopped, esList[0].Status)

	// Get the first by ID
	es1c, err := mgr.GetStreamByID(ctx, es1.ID, dbsql.FailIfNotFound)
	assert.NoError(t, err)
	assert.Equal(t, "stream1", *es1c.Name)
	assert.Equal(t, EventStreamStatusStarted, es1c.Status)

	// Start and re-stop, then delete the second event stream
	err = mgr.StartStream(ctx, es2.ID)
	assert.NoError(t, err)
	es2c, err := mgr.GetStreamByID(ctx, es2.ID, dbsql.FailIfNotFound)
	assert.NoError(t, err)
	assert.Equal(t, EventStreamStatusStarted, es2c.Status)
	err = mgr.StopStream(ctx, es2.ID)
	assert.NoError(t, err)
	es2c, err = mgr.GetStreamByID(ctx, es2.ID, dbsql.FailIfNotFound)
	assert.NoError(t, err)
	assert.Equal(t, EventStreamStatusStopped, es2c.Status)
	err = mgr.DeleteStream(ctx, es2.ID)
	assert.NoError(t, err)

	// Delete the first stream (which is running still)
	err = mgr.DeleteStream(ctx, es1.ID)
	assert.NoError(t, err)

	// Check no streams left
	esList, _, err = mgr.ListStreams(ctx, EventStreamFilters.NewFilter(ctx).And())
	assert.NoError(t, err)
	assert.Empty(t, esList)

}

func setupE2ETest(t *testing.T) (context.Context, Persistence[testESConfig], wsserver.WebSocketChannels, wsclient.WSClient, func()) {
	logrus.SetLevel(logrus.TraceLevel)

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

	p := NewEventStreamPersistence[testESConfig](db)
	p.EventStreams().Validate()
	p.Checkpoints().Validate()

	wss := wsserver.NewWebSocketServer(ctx)
	server := httptest.NewServer(http.HandlerFunc(wss.Handler))

	// Build the WS connection, but don't connect it yet
	wsc, err := wsclient.New(ctx, &wsclient.WSConfig{
		HTTPURL: server.URL,
	}, nil, nil)

	return ctx, p, wss, wsc, func() {
		server.Close()
		wsc.Close()
	}

}
