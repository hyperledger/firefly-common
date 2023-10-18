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
	"github.com/hyperledger/firefly-common/pkg/dbsql"
	"github.com/hyperledger/firefly-common/pkg/ffapi"
)

type Persistence[CT any] interface {
	EventStreams() dbsql.CRUD[*EventStreamSpec[CT]]
	Checkpoints() dbsql.CRUD[*EventStreamCheckpoint]
	Close()
}

var EventStreamFilters = &ffapi.QueryFields{
	"id":          &ffapi.UUIDField{},
	"created":     &ffapi.TimeField{},
	"updated":     &ffapi.TimeField{},
	"name":        &ffapi.StringField{},
	"status":      &ffapi.StringField{},
	"type":        &ffapi.StringField{},
	"topicFilter": &ffapi.StringField{},
}
