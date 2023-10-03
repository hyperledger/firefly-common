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
	"database/sql/driver"

	"github.com/hyperledger/firefly-common/pkg/dbsql"
	"github.com/hyperledger/firefly-common/pkg/fftypes"
)

type DistributionMode = fftypes.FFEnum

var (
	DistributionModeBroadcast   = fftypes.FFEnumValue("distmode", "broadcast")
	DistributionModeLoadBalance = fftypes.FFEnumValue("distmode", "load_balance")
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

type EventStream struct {
	dbsql.ResourceBase
	Name      *string          `ffstruct:"eventstream" json:"name,omitempty"`
	Suspended *bool            `ffstruct:"eventstream" json:"suspended,omitempty"`
	Type      *EventStreamType `ffstruct:"eventstream" json:"type,omitempty" ffenum:"estype"`

	ErrorHandling     *ErrorHandlingType  `ffstruct:"eventstream" json:"errorHandling"`
	BatchSize         *uint64             `ffstruct:"eventstream" json:"batchSize"`
	BatchTimeout      *fftypes.FFDuration `ffstruct:"eventstream" json:"batchTimeout"`
	RetryTimeout      *fftypes.FFDuration `ffstruct:"eventstream" json:"retryTimeout"`
	BlockedRetryDelay *fftypes.FFDuration `ffstruct:"eventstream" json:"blockedRetryDelay"`

	Webhook   *WebhookConfig   `ffstruct:"eventstream" json:"webhook,omitempty"`
	WebSocket *WebSocketConfig `ffstruct:"eventstream" json:"websocket,omitempty"`
}

func (es *EventStream) GetID() string {
	return es.ID.String()
}

func (es *EventStream) SetCreated(t *fftypes.FFTime) {
	es.Created = t
}

func (es *EventStream) SetUpdated(t *fftypes.FFTime) {
	es.Updated = t
}

type EventStreamStatus string

const (
	EventStreamStatusStarted  EventStreamStatus = "started"
	EventStreamStatusStopping EventStreamStatus = "stopping"
	EventStreamStatusStopped  EventStreamStatus = "stopped"
	EventStreamStatusDeleted  EventStreamStatus = "deleted"
)

type EventStreamWithStatus struct {
	EventStream
	Status EventStreamStatus `ffstruct:"eventstream" json:"status"`
}

type EventStreamCheckpoint struct {
	dbsql.ResourceBase
	Value *fftypes.JSONAny `json:"value,omitempty"`
}

type WebSocketConfig struct {
	DistributionMode *DistributionMode `ffstruct:"wsconfig" json:"distributionMode,omitempty"`
}

// Store in DB as JSON
func (wc *WebSocketConfig) Scan(src interface{}) error {
	return fftypes.JSONScan(src, wc)
}

// Store in DB as JSON
func (wc *WebSocketConfig) Value() (driver.Value, error) {
	if wc == nil {
		return nil, nil
	}
	return fftypes.JSONValue(wc)
}
