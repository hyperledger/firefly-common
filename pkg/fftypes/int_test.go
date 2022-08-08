// Copyright © 2022 Kaleido, Inc.
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
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFFInt64JSON(t *testing.T) {

	var myStruct struct {
		Field1 FFint64  `json:"field1"`
		Field2 *FFint64 `json:"field2"`
		Field3 *FFint64 `json:"field3"`
		Field4 *FFint64 `json:"field4"`
	}

	jsonVal := []byte(`{
		"field1": -111111,
		"field2": 2222.22,
		"field3": "333333",
		"field4": "0xfeedBEEF"
	}`)

	err := json.Unmarshal(jsonVal, &myStruct)
	assert.NoError(t, err)
	assert.Equal(t, int64(-111111), myStruct.Field1.Int64())
	assert.Equal(t, int64(2222), myStruct.Field2.Int64())
	assert.Equal(t, int64(333333), myStruct.Field3.Int64())
	assert.Equal(t, int64(4276993775), myStruct.Field4.Int64())

	jsonValSerialized, err := json.Marshal(&myStruct)

	assert.NoError(t, err)
	assert.JSONEq(t, `{
		"field1": "-111111",
		"field2": "2222",
		"field3": "333333",
		"field4": "4276993775"
	}`, string(jsonValSerialized))

	var ffi *FFuint64
	assert.Equal(t, uint64(0), ffi.Uint64())
	err = ffi.UnmarshalJSON([]byte(`"bad string`))
	assert.Regexp(t, "FF00104", err)
	err = ffi.UnmarshalJSON([]byte(`"!!! not a number"`))
	assert.Regexp(t, "FF00104", err)
	err = ffi.UnmarshalJSON([]byte(`{}`))
	assert.Regexp(t, "FF00104", err)
}

func TestFFUint64JSON(t *testing.T) {

	var myStruct struct {
		Field1 FFuint64  `json:"field1"`
		Field2 *FFuint64 `json:"field2"`
		Field3 *FFuint64 `json:"field3"`
		Field4 *FFuint64 `json:"field4"`
	}

	jsonVal := []byte(`{
		"field1": -111111,
		"field2": 2222.22,
		"field3": "333333",
		"field4": "0xfeedBEEF"
	}`)

	err := json.Unmarshal(jsonVal, &myStruct)
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), myStruct.Field1.Uint64())
	assert.Equal(t, uint64(2222), myStruct.Field2.Uint64())
	assert.Equal(t, uint64(333333), myStruct.Field3.Uint64())
	assert.Equal(t, uint64(4276993775), myStruct.Field4.Uint64())

	jsonValSerialized, err := json.Marshal(&myStruct)

	assert.NoError(t, err)
	assert.JSONEq(t, `{
		"field1": "0",
		"field2": "2222",
		"field3": "333333",
		"field4": "4276993775"
	}`, string(jsonValSerialized))

	var ffi *FFint64
	assert.Equal(t, int64(0), ffi.Int64())
	err = ffi.UnmarshalJSON([]byte(`"bad string`))
	assert.Regexp(t, "FF00104", err)
	err = ffi.UnmarshalJSON([]byte(`"!!! not a number"`))
	assert.Regexp(t, "FF00104", err)
	err = ffi.UnmarshalJSON([]byte(`{}`))
	assert.Regexp(t, "FF00104", err)
}
