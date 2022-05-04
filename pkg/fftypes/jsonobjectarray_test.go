// Copyright © 2021 Kaleido, Inc.
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
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestJSONObjectArray(t *testing.T) {

	data := JSONAnyPtr(`{
		"field1": true,
		"field2": false,
		"field3": "True",
		"field4": "not true",
		"field5": { "not": "boolable" },
		"field6": null
	}`)
	dataJSON := data.JSONObject()
	assert.True(t, dataJSON.GetBool("field1"))
	assert.False(t, dataJSON.GetBool("field2"))
	assert.True(t, dataJSON.GetBool("field3"))
	assert.False(t, dataJSON.GetBool("field4"))
	assert.False(t, dataJSON.GetBool("field5"))
	assert.False(t, dataJSON.GetBool("field6"))
	assert.False(t, dataJSON.GetBool("field7"))
}

func TestJSONObjectArrayScan(t *testing.T) {

	var joa JSONObjectArray

	err := joa.Scan(`[{"test": 1}]`)
	assert.NoError(t, err)
	assert.Equal(t, "1", joa[0].GetString("test"))

	err = joa.Scan([]byte(`[{"test": 1}]`))
	assert.NoError(t, err)
	assert.Equal(t, "1", joa[0].GetString("test"))

	err = joa.Scan(nil)
	assert.NoError(t, err)
	assert.Empty(t, joa)

	err = joa.Scan("")
	assert.NoError(t, err)
	assert.Empty(t, joa)

	err = joa.Scan([]byte(nil))
	assert.NoError(t, err)
	assert.Empty(t, joa)

	joa = JSONObjectArray([]JSONObject{
		JSONObject(map[string]interface{}{
			"bad": map[bool]bool{false: true},
		}),
	})
	_, err = joa.Value()
	assert.Error(t, err)

}

func TestJSONObjectArrayScanExtra(t *testing.T) {

	data := JSONObjectArray{{"some": "data"}}

	sv, err := data.Value()
	assert.NoError(t, err)
	assert.Equal(t, "[{\"some\":\"data\"}]", sv)

	var dataRead JSONObjectArray
	err = dataRead.Scan(sv)
	assert.NoError(t, err)

	assert.Equal(t, `[{"some":"data"}]`, fmt.Sprintf("%v", dataRead))

	var badData = JSONObjectArray{map[string]interface{}{"bad": map[bool]bool{false: true}}}
	_, err = badData.Value()
	assert.Error(t, err)

	j1, err := json.Marshal(&data)
	assert.NoError(t, err)
	j2, err := json.Marshal(&dataRead)
	assert.NoError(t, err)
	assert.Equal(t, string(j1), string(j2))
	j3 := dataRead.String()
	assert.Equal(t, string(j1), j3)

	err = dataRead.Scan("")
	assert.NoError(t, err)

	err = dataRead.Scan([]byte("[{}]"))
	assert.NoError(t, err)

	err = dataRead.Scan(`[{"test": true}]`)
	assert.NoError(t, err)
	assert.True(t, dataRead[0].GetBool("test"))

	err = dataRead.Scan(nil)
	assert.NoError(t, err)

	var wrongType int
	err = dataRead.Scan(&wrongType)
	assert.Error(t, err)

	hash, err := dataRead.Hash("goodStuff")
	assert.NoError(t, err)
	assert.NotEmpty(t, hash)

	var badJson JSONObjectArray = []JSONObject{{"not": map[bool]string{true: "json"}}}
	hash, err = badJson.Hash("badStuff")
	assert.Regexp(t, "FF00127.*badStuff", err)
	assert.Nil(t, hash)

}
