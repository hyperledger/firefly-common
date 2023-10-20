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

package fftypes

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestJSONScan(t *testing.T) {

	type TS struct {
		Val1 string `json:"val1"`
		Val2 string `json:"val2,omitempty"`
	}

	ts1 := TS{}
	err := JSONScan(`{"val1":"111","val2":"222"}`, &ts1)
	assert.NoError(t, err)
	assert.Equal(t, TS{Val1: "111", Val2: "222"}, ts1)

	ts2 := TS{}
	err = JSONScan([]byte(`{"val1":"111","val2":"222"}`), &ts2)
	assert.NoError(t, err)
	assert.Equal(t, TS{Val1: "111", Val2: "222"}, ts2)

	ts3 := TS{}
	err = JSONScan(nil, &ts3)
	assert.NoError(t, err)

	ts4 := TS{}
	err = JSONScan(12345, &ts4)
	assert.Regexp(t, "FF00215", err)

}

func TestJSONValue(t *testing.T) {

	type TS struct {
		Val1 string `json:"val1"`
		Val2 string `json:"val2,omitempty"`
	}

	v, err := JSONValue(TS{Val1: "111", Val2: "222"})
	assert.NoError(t, err)
	assert.JSONEq(t, `{"val1":"111","val2":"222"}`, string(v.([]byte)))

	var ts *TS
	v, err = JSONValue(ts)
	assert.NoError(t, err)
	assert.Nil(t, v)
}
