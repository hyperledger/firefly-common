// Copyright © 2024 Kaleido, Inc.
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

	sq "github.com/Masterminds/squirrel"
	"github.com/hyperledger/firefly-common/pkg/dbsql"
	"github.com/hyperledger/firefly-common/pkg/ffapi"
)

type Persistence[CT any] interface {
	EventStreams() dbsql.CRUD[*EventStreamSpec[CT]]
	Checkpoints() dbsql.CRUD[*EventStreamCheckpoint]
	Close()
}

var EventStreamFilters = &ffapi.QueryFields{
	"id":          &ffapi.StringField{},
	"created":     &ffapi.TimeField{},
	"updated":     &ffapi.TimeField{},
	"name":        &ffapi.StringField{},
	"status":      &ffapi.StringField{},
	"type":        &ffapi.StringField{},
	"topicfilter": &ffapi.StringField{},
	"identity":    &ffapi.StringField{},
	"config":      &ffapi.JSONField{},
}

var CheckpointFilters = &ffapi.QueryFields{
	"id":         &ffapi.StringField{},
	"created":    &ffapi.TimeField{},
	"updated":    &ffapi.TimeField{},
	"sequenceid": &ffapi.StringField{},
}

type IDValidator func(ctx context.Context, idStr string) error

func NewEventStreamPersistence[CT any](db *dbsql.Database, idValidator IDValidator) Persistence[CT] {
	return &esPersistence[CT]{
		db:          db,
		idValidator: idValidator,
	}
}

type esPersistence[CT any] struct {
	db          *dbsql.Database
	idValidator IDValidator
}

func (p *esPersistence[CT]) EventStreams() dbsql.CRUD[*EventStreamSpec[CT]] {
	return &dbsql.CrudBase[*EventStreamSpec[CT]]{
		DB:    p.db,
		Table: "eventstreams",
		Columns: []string{
			dbsql.ColumnID,
			dbsql.ColumnCreated,
			dbsql.ColumnUpdated,
			"name",
			"status",
			"type",
			"initial_sequence_id",
			"topic_filter",
			"identity",
			"config",
			"error_handling",
			"batch_size",
			"batch_timeout",
			"retry_timeout",
			"blocked_retry_delay",
			"webhook_config",
			"websocket_config",
		},
		FilterFieldMap: map[string]string{
			"topicfilter": "topic_filter",
		},
		NilValue:     func() *EventStreamSpec[CT] { return nil },
		NewInstance:  func() *EventStreamSpec[CT] { return &EventStreamSpec[CT]{} },
		ScopedFilter: func() sq.Eq { return sq.Eq{} },
		EventHandler: nil, // set below
		NameField:    "name",
		QueryFactory: EventStreamFilters,
		IDValidator:  p.idValidator,
		GetFieldPtr: func(inst *EventStreamSpec[CT], col string) interface{} {
			switch col {
			case dbsql.ColumnID:
				return &inst.ID
			case dbsql.ColumnCreated:
				return &inst.Created
			case dbsql.ColumnUpdated:
				return &inst.Updated
			case "name":
				return &inst.Name
			case "status":
				return &inst.Status
			case "type":
				return &inst.Type
			case "initial_sequence_id":
				return &inst.InitialSequenceID
			case "topic_filter":
				return &inst.TopicFilter
			case "identity":
				return &inst.Identity
			case "config":
				return &inst.Config
			case "error_handling":
				return &inst.ErrorHandling
			case "batch_size":
				return &inst.BatchSize
			case "batch_timeout":
				return &inst.BatchTimeout
			case "retry_timeout":
				return &inst.RetryTimeout
			case "blocked_retry_delay":
				return &inst.BlockedRetryDelay
			case "webhook_config":
				return &inst.Webhook
			case "websocket_config":
				return &inst.WebSocket
			}
			return nil
		},
	}
}

func (p *esPersistence[CT]) Checkpoints() dbsql.CRUD[*EventStreamCheckpoint] {
	return &dbsql.CrudBase[*EventStreamCheckpoint]{
		DB:    p.db,
		Table: "es_checkpoints",
		Columns: []string{
			dbsql.ColumnID,
			dbsql.ColumnCreated,
			dbsql.ColumnUpdated,
			"sequence_id",
		},
		FilterFieldMap: map[string]string{
			"sequenceid": "sequence_id",
		},
		NilValue:     func() *EventStreamCheckpoint { return nil },
		NewInstance:  func() *EventStreamCheckpoint { return &EventStreamCheckpoint{} },
		ScopedFilter: func() sq.Eq { return sq.Eq{} },
		EventHandler: nil, // set below
		QueryFactory: CheckpointFilters,
		IDValidator:  p.idValidator, // checkpoints share the ID of the eventstream
		GetFieldPtr: func(inst *EventStreamCheckpoint, col string) interface{} {
			switch col {
			case dbsql.ColumnID:
				return &inst.ID
			case dbsql.ColumnCreated:
				return &inst.Created
			case dbsql.ColumnUpdated:
				return &inst.Updated
			case "sequence_id":
				return &inst.SequenceID
			}
			return nil
		},
	}
}

func (p *esPersistence[CT]) Close() {
	p.db.Close()
}
