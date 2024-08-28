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
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestJSONAnySerializeNull(t *testing.T) {

	type testStruct struct {
		Prop1 *JSONAny `json:"prop1"`
		Prop2 *JSONAny `json:"prop2,omitempty"`
	}

	ts := &testStruct{}

	err := json.Unmarshal([]byte(`{}`), &ts)
	assert.NoError(t, err)
	assert.Nil(t, ts.Prop1)
	assert.Nil(t, ts.Prop2)
	b, err := json.Marshal(&ts)
	assert.Equal(t, `{"prop1":null}`, string(b))

}

func TestJSONAnySerializeObjects(t *testing.T) {

	type testStruct struct {
		Prop *JSONAny `json:"prop,omitempty"`
	}

	ts := &testStruct{}

	err := json.Unmarshal([]byte(`{"prop":{"b":"test","b":"duplicate","a":12345,"c":{"e":1.00000000001,"d":false}}}`), &ts)
	assert.NoError(t, err)
	b, err := json.Marshal(&ts)
	assert.NoError(t, err)
	assert.Equal(t, `{"prop":{"b":"test","b":"duplicate","a":12345,"c":{"e":1.00000000001,"d":false}}}`, string(b))
	err = json.Unmarshal([]byte(`{"prop"
		:{"b":"test", "b":"duplicate","a" 				:12345,"c":{
			  "e":1.00000000001,
		"d":false}
	}}`), &ts)
	b, err = json.Marshal(&ts)
	assert.NoError(t, err)
	assert.Equal(t, `{"prop":{"b":"test","b":"duplicate","a":12345,"c":{"e":1.00000000001,"d":false}}}`, string(b))
	b, err = json.Marshal(&ts.Prop)
	assert.Equal(t, `{"b":"test","b":"duplicate","a":12345,"c":{"e":1.00000000001,"d":false}}`, string(b))
	assert.Equal(t, "8eff3083f052a77bda0934236bf8e5eccbd186d5ae81ada7a5bbee516ecd5726", ts.Prop.Hash().String())

	jo, ok := ts.Prop.JSONObjectOk()
	assert.True(t, ok)
	assert.Equal(t, "duplicate", jo.GetString("b"))

	assert.Empty(t, "", ((*JSONAny)(nil)).JSONObject().GetString("notThere"))

}

func TestJSONAnyMarshalNull(t *testing.T) {

	var pb JSONAny
	b, err := pb.MarshalJSON()
	assert.NoError(t, err)
	assert.Equal(t, NullString, string(b))
	assert.Equal(t, NullString, pb.String())
	assert.True(t, pb.IsNil())

	err = pb.UnmarshalJSON([]byte(""))
	assert.NoError(t, err)
	assert.True(t, pb.IsNil())

	var ppb *JSONAny
	assert.Equal(t, NullString, ppb.String())
	assert.True(t, pb.IsNil())

}

func TestJSONAnyUnmarshalFail(t *testing.T) {

	var b JSONAny
	err := b.UnmarshalJSON([]byte(`!json`))
	assert.Error(t, err)

	jo := b.JSONObject()
	assert.Equal(t, JSONObject{}, jo)
}

func TestScan(t *testing.T) {

	var h JSONAny
	assert.Equal(t, int64(0), h.Length())
	assert.NoError(t, h.Scan(nil))
	assert.Empty(t, h)

	assert.NoError(t, h.Scan(`{"some": "stuff"}`))
	assert.Equal(t, "stuff", h.JSONObject().GetString("some"))

	assert.NoError(t, h.Scan([]byte(`{"some": "stuff"}`)))
	assert.Equal(t, "stuff", h.JSONObject().GetString("some"))

	assert.NoError(t, h.Scan(`"plainstring"`))
	assert.Equal(t, "", h.JSONObjectNowarn().GetString("some"))

	assert.Regexp(t, "FF00105", h.Scan(12345))

	assert.Equal(t, "test", JSONAnyPtrBytes([]byte(`{"val": "test"}`)).JSONObject().GetString("val"))
	assert.Nil(t, JSONAnyPtrBytes(nil))
	assert.Equal(t, int64(0), JSONAnyPtrBytes(nil).Length())

	assert.Nil(t, JSONAnyPtrBytes(nil).Bytes())
	assert.NotEmpty(t, JSONAnyPtr("{}").Bytes())
	assert.Equal(t, int64(2), JSONAnyPtr("{}").Length())

}

func TestJSONAnyToJSONObjectArray(t *testing.T) {

	var h JSONAny

	assert.NoError(t, h.Scan(`[{"some": "stuff"}]`))
	assert.Equal(t, "stuff", h.JSONObjectArray()[0].GetString("some"))

	assert.NoError(t, h.Scan(`{"some": "stuff"}`))
	assert.Empty(t, h.JSONObjectArray())

}

func TestValue(t *testing.T) {

	var h *JSONAny
	v, err := h.Value()
	assert.NoError(t, err)
	assert.Nil(t, v)
	err = h.Scan(v)
	assert.NoError(t, err)
	assert.Nil(t, h)

	h = JSONAnyPtr("")
	v, err = h.Value()
	assert.NoError(t, err)
	assert.Nil(t, v)

	h = JSONAnyPtr("{}")
	v, err = h.Value()
	assert.NoError(t, err)
	assert.Equal(t, "{}", v)

}

func TestUnmarshal(t *testing.T) {

	var h *JSONAny
	var myObj struct {
		Key1 string `json:"key1"`
	}
	err := h.Unmarshal(context.Background(), &myObj)
	assert.Regexp(t, "FF00125", err)

	h = JSONAnyPtr(`{"key1":"value1"}`)
	err = h.Unmarshal(context.Background(), &myObj)
	assert.NoError(t, err)
	assert.Equal(t, "value1", myObj.Key1)
}

func TestUnmarshalHugeNumber(t *testing.T) {

	var myInt64Variable int64
	var myFloat64Variable float64
	ctx := context.Background()
	var h *JSONAny
	var myObj struct {
		Key1 interface{} `json:"key1"`
		Key2 JSONAny     `json:"key2"`
		Key3 JSONAny     `json:"key3"`
	}

	h = JSONAnyPtr(`{"key1":123456789123456789123456789, "key2":123456789123456789123456789, "key3":1234}`)
	err := h.Unmarshal(ctx, &myObj)
	assert.NoError(t, err)
	assert.Equal(t, json.Number("123456789123456789123456789"), myObj.Key1)

	assert.NoError(t, err)
	assert.Equal(t, "123456789123456789123456789", myObj.Key2.String())

	err = myObj.Key2.Unmarshal(ctx, &myInt64Variable)
	assert.Error(t, err)
	assert.Regexp(t, "cannot unmarshal number 123456789123456789123456789 into Go value of type int64", err)

	err = myObj.Key3.Unmarshal(ctx, &myInt64Variable)
	assert.NoError(t, err)
	assert.Equal(t, int64(1234), myInt64Variable)

	err = myObj.Key2.Unmarshal(ctx, &myFloat64Variable)
	assert.Error(t, err)
	assert.Regexp(t, "FF00249", err)
}

func TestUnmarshalHugeNumberError(t *testing.T) {

	var h *JSONAny
	var myObj struct {
		Key1 interface{} `json:"key1"`
	}

	h = JSONAnyPtr(`{"key1":1234567891invalidchars234569}`)
	err := h.Unmarshal(context.Background(), &myObj)
	assert.Error(t, err)
}

func TestNilHash(t *testing.T) {
	assert.Nil(t, (*JSONAny)(nil).Hash())
}

func TestASString(t *testing.T) {
	j := JSONAnyPtr("\"foo\"")
	assert.Equal(t, "foo", j.AsString())

	nj := (*JSONAny)(nil)
	assert.Equal(t, "null", nj.AsString())
}
