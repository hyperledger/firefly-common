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

import "github.com/hyperledger/firefly-common/pkg/fftypes"

type EventBatch[DataType any] struct {
	StreamID    *fftypes.UUID      `json:"stream"`      // the ID of the event stream for this event
	BatchNumber int64              `json:"batchNumber"` // should be provided back in the ack
	Events      []*Event[DataType] `json:"events"`      // an array of events allows efficient batch acknowledgment
}

type Event[DataType any] struct {
	Topic      string    `json:"topic,omitempty"` // describes the sub-stream of events (optional) allowing sever-side event filtering (regexp)
	SequenceID string    `json:"sequenceId"`      // deterministic ID for the event, that must be alpha-numerically orderable within the stream (numbers must be left-padded hex/decimal strings for ordering)
	Data       *DataType `json:",inline"`         // can be anything to deliver for the event - must be JSON marshalable, and should not define topic or sequence
}
