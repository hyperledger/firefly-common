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
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMarshalFlatteningOK(t *testing.T) {
	type myEvent struct {
		Field1 string `json:"field1"`
	}
	e := Event[myEvent]{
		EventCommon: EventCommon{
			Topic:      "topic1",
			SequenceID: "11111",
		},
		Data: &myEvent{
			Field1: "val1111",
		},
	}
	d, err := json.Marshal(&e)
	assert.NoError(t, err)
	assert.JSONEq(t, `{
		"topic": "topic1",
		"sequenceId": "11111",
		"field1": "val1111"
	}`, string(d))

	var e2 Event[myEvent]
	err = json.Unmarshal(d, &e2)
	assert.NoError(t, err)
	assert.Equal(t, e, e2)
}

func TestMarshalFlatteningFail(t *testing.T) {
	type myEvent struct {
		Badness map[bool]bool
	}
	e := Event[myEvent]{
		Data: &myEvent{
			Badness: map[bool]bool{false: true},
		},
	}
	_, err := json.Marshal(&e)
	assert.Error(t, err)
}

func TestUnmarshalFlatteningFail(t *testing.T) {
	var e Event[string]
	err := json.Unmarshal([]byte(`{"topic":false}`), &e)
	assert.Error(t, err)
}
