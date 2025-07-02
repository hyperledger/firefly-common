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

func TestJSONObject(t *testing.T) {

	data := JSONObject{
		"some": "data",
	}

	b, err := data.Value()
	assert.NoError(t, err)
	assert.IsType(t, "", b)

	var dataRead JSONObject
	err = dataRead.Scan(b)
	assert.NoError(t, err)

	assert.Equal(t, `{"some":"data"}`, fmt.Sprintf("%v", dataRead))

	j1, err := json.Marshal(&data)
	assert.NoError(t, err)
	j2, err := json.Marshal(&dataRead)
	assert.NoError(t, err)
	assert.Equal(t, string(j1), string(j2))
	j3 := dataRead.String()
	assert.Equal(t, string(j1), j3)

	err = dataRead.Scan("")
	assert.NoError(t, err)

	err = dataRead.Scan(nil)
	assert.NoError(t, err)

	var wrongType int
	err = dataRead.Scan(&wrongType)
	assert.Error(t, err)

	hash, err := dataRead.Hash("goodStuff")
	assert.NoError(t, err)
	assert.NotEmpty(t, hash)

	var badJson JSONObject = map[string]interface{}{"not": map[bool]string{true: "json"}}
	hash, err = badJson.Hash("badStuff")
	assert.Regexp(t, "FF00127.*badStuff", err)
	assert.Nil(t, hash)

	v, ok := JSONObject{"test": false}.GetStringOk("test")
	assert.True(t, ok)
	assert.Equal(t, "false", v)

	v, ok = JSONObject{"test": float64(12345)}.GetStringOk("test")
	assert.True(t, ok)
	assert.Equal(t, "12345", v)

	v, ok = JSONObject{}.GetStringOk("test")
	assert.False(t, ok)
	assert.Equal(t, "", v)

	v, ok = JSONObject{"test": map[string]int{"x": 12345}}.GetStringOk("test")
	assert.False(t, ok)
	assert.Equal(t, "", v)
}

func TestGetInt64NumberTypes(t *testing.T) {
	var numberVals JSONObject = map[string]interface{}{
		"v0": int(123),
		"v1": int8(123),
		"v2": int16(123),
		"v3": int32(123),
		"v4": int64(123),
		"v5": uint(123),
		"v6": uint8(123),
		"v7": uint16(123),
		"v8": uint32(123),
		"v9": uint64(123),
	}
	assert.Equal(t, int64(123), numberVals.GetInt64("v0"))
	assert.Equal(t, int64(123), numberVals.GetInt64("v1"))
	assert.Equal(t, int64(123), numberVals.GetInt64("v2"))
	assert.Equal(t, int64(123), numberVals.GetInt64("v3"))
	assert.Equal(t, int64(123), numberVals.GetInt64("v4"))
	assert.Equal(t, int64(123), numberVals.GetInt64("v5"))
	assert.Equal(t, int64(123), numberVals.GetInt64("v6"))
	assert.Equal(t, int64(123), numberVals.GetInt64("v7"))
	assert.Equal(t, int64(123), numberVals.GetInt64("v8"))
	assert.Equal(t, int64(123), numberVals.GetInt64("v9"))
}

func TestGetStringFloatNumberTypes(t *testing.T) {
	var numberVals JSONObject = map[string]interface{}{
		"v0": float32(123.4),
		"v1": float64(123.4),
	}
	assert.Regexp(t, "123.4.*", numberVals.GetString("v0"))
	assert.Equal(t, "123.4", numberVals.GetString("v1"))
}

func TestJSONObjectScan(t *testing.T) {

	data := JSONObject{"some": "data"}

	sv, err := data.Value()
	assert.NoError(t, err)
	assert.Equal(t, "{\"some\":\"data\"}", sv)

	var dataRead JSONObject
	err = dataRead.Scan(sv)
	assert.NoError(t, err)

	assert.Equal(t, `{"some":"data"}`, fmt.Sprintf("%v", dataRead))

	sv, err = ((JSONObject)(nil)).Value()
	assert.NoError(t, err)
	assert.Nil(t, sv)

	var badData JSONObject = map[string]interface{}{"bad": map[bool]bool{false: true}}
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

	err = dataRead.Scan([]byte("{}"))
	assert.NoError(t, err)

	err = dataRead.Scan(`{"test": true}`)
	assert.NoError(t, err)
	assert.True(t, dataRead.GetBool("test"))

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

func TestJSONNestedSafeGet(t *testing.T) {

	var jd JSONObject
	err := json.Unmarshal([]byte(`
		{
			"nested_array": [
				{
					"with": {
						"some": "value"
					}
				}
			],
			"string_array": ["str1","str2"],
			"wrong": null,
			"int1": "0xfeedbeef",
			"int2": "12345",
			"bad": "!!!!!!!not an integer"
		}
	`), &jd)
	assert.NoError(t, err)

	va, ok := jd.GetObjectArrayOk("wrong")
	assert.False(t, ok)
	assert.NotNil(t, va)

	vo, ok := jd.GetObjectOk("wrong")
	assert.False(t, ok)
	assert.NotNil(t, vo)
	vo, ok = jd.GetObjectOk("string_array")
	assert.False(t, ok)
	assert.NotNil(t, vo)
	jd["already_typed"] = JSONObject{"some": "stuff"}
	vo, ok = jd.GetObjectOk("already_typed")
	assert.True(t, ok)
	assert.Equal(t, "stuff", vo.GetString("some"))

	assert.Equal(t, "value",
		jd.GetObjectArray("nested_array")[0].
			GetObject("with").
			GetString("some"),
	)

	assert.Equal(t, int64(0xfeedbeef), jd.GetInt64("int1"))
	assert.Equal(t, int64(12345), jd.GetInt64("int2"))
	assert.Equal(t, int64(0), jd.GetInt64("wrong"))
	assert.Equal(t, int64(0), jd.GetInteger("string_array").Int64())
	assert.Equal(t, int64(0), jd.GetInteger("bad").Int64())

	sa, ok := jd.GetStringArrayOk("wrong")
	assert.False(t, ok)
	assert.Empty(t, sa)

	sa, ok = jd.GetStringArrayOk("string_array")
	assert.True(t, ok)
	assert.Equal(t, []string{"str1", "str2"}, sa)

	assert.Equal(t, []string{"str1", "str2"}, jd.GetStringArray("string_array"))

	sa, ok = ToStringArray(jd.GetStringArray("string_array"))
	assert.True(t, ok)
	assert.Equal(t, []string{"str1", "str2"}, sa)

	remapped, ok := ToJSONObjectArray(jd.GetObjectArray("nested_array"))
	assert.True(t, ok)
	assert.Equal(t, "value",
		remapped[0].
			GetObject("with").
			GetString("some"),
	)

	assert.Equal(t, "",
		jd.GetObject("no").
			GetObject("path").
			GetObject("to").
			GetString("here"),
	)

}

func TestDeepCopyBasic(t *testing.T) {
	original := JSONObject{
		"key1": "value1",
		"key2": 123,
		"key3": true,
	}

	copy := original.DeepCopy()

	assert.Equal(t, original, copy)
	assert.NotSame(t, original, copy)
}

func TestDeepCopyNested(t *testing.T) {
	original := JSONObject{
		"key1": JSONObject{
			"nestedKey1": "nestedValue1",
			"nestedKey2": 456,
		},
		"key2": []interface{}{"elem1", "elem2"},
	}

	copy := original.DeepCopy()

	assert.Equal(t, original, copy)
	assert.NotSame(t, original, copy)
	assert.NotSame(t, original["key1"], copy["key1"])
	assert.NotSame(t, original["key2"], copy["key2"])
}

func TestDeepCopyEmpty(t *testing.T) {
	original := JSONObject{}

	copy := original.DeepCopy()

	assert.Equal(t, original, copy)
	assert.NotSame(t, original, copy)
}

func TestDeepCopyNil(t *testing.T) {
	var original JSONObject = nil

	copy := original.DeepCopy()

	assert.Nil(t, copy)
}
