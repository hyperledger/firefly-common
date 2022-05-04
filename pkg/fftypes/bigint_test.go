// Copyright © 2021 Kaleido, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, souware
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package fftypes

import (
	"encoding/json"
	"math/big"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBigIntEmptyJSON(t *testing.T) {

	var myStruct struct {
		Field1 FFBigInt  `json:"field1,omitempty"`
		Field2 *FFBigInt `json:"field2,omitempty"`
		Field3 *FFBigInt `json:"field3"`
	}

	jsonVal := []byte(`{}`)

	err := json.Unmarshal(jsonVal, &myStruct)
	assert.NoError(t, err)
	assert.Zero(t, myStruct.Field1.Int().Int64())
	assert.Nil(t, myStruct.Field2)
	assert.Nil(t, myStruct.Field3)

}

func TestBigIntSetJSONOk(t *testing.T) {

	var myStruct struct {
		Field1 FFBigInt  `json:"field1"`
		Field2 *FFBigInt `json:"field2"`
		Field3 *FFBigInt `json:"field3"`
		Field4 *FFBigInt `json:"field4"`
	}

	jsonVal := []byte(`{
		"field1": -111111,
		"field2": 2222.22,
		"field3": "333333",
		"field4": "0xfeedBEEF"
	}`)

	err := json.Unmarshal(jsonVal, &myStruct)
	assert.NoError(t, err)
	assert.Equal(t, int64(-111111), myStruct.Field1.Int().Int64())
	assert.Equal(t, int64(2222), myStruct.Field2.Int().Int64())
	assert.Equal(t, int64(333333), myStruct.Field3.Int().Int64())
	assert.Equal(t, int64(4276993775), myStruct.Field4.Int().Int64())

	jsonValSerialized, err := json.Marshal(&myStruct)

	assert.NoError(t, err)
	assert.JSONEq(t, `{
		"field1": "-111111",
		"field2": "2222",
		"field3": "333333",
		"field4": "4276993775"
	}`, string(jsonValSerialized))
}

func TestBigIntJSONBadString(t *testing.T) {

	jsonVal := []byte(`"0xZZ"`)

	var bi FFBigInt
	err := json.Unmarshal(jsonVal, &bi)
	assert.Regexp(t, "FF00104", err)

}

func TestBigIntJSONBadType(t *testing.T) {

	jsonVal := []byte(`{
		"field1": { "not": "valid" }
	}`)

	var bi FFBigInt
	err := json.Unmarshal(jsonVal, &bi)
	assert.Regexp(t, "FF00104", err)

}

func TestBigIntJSONBadJSON(t *testing.T) {

	jsonVal := []byte(`!JSON`)

	var bi FFBigInt
	err := bi.UnmarshalJSON(jsonVal)
	assert.Regexp(t, "FF00104", err)

}

func TestLagePositiveBigIntValue(t *testing.T) {

	var iMax FFBigInt
	_ = iMax.Int().Exp(big.NewInt(2), big.NewInt(256), nil)
	iMax.Int().Sub(iMax.Int(), big.NewInt(1))
	iMaxVal, err := iMax.Value()
	assert.NoError(t, err)
	assert.Equal(t, "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff", iMaxVal)

	var iRead big.Int
	_, ok := iRead.SetString(iMaxVal.(string), 16)
	assert.True(t, ok)

}

func TestLargeNegativeBigIntValue(t *testing.T) {

	var iMax FFBigInt
	_ = iMax.Int().Exp(big.NewInt(2), big.NewInt(256), nil)
	iMax.Int().Neg(iMax.Int())
	iMax.Int().Add(iMax.Int(), big.NewInt(1))
	iMaxVal, err := iMax.Value()
	assert.NoError(t, err)
	// Note that this is a "-" prefix with a variable width big-endian positive number (not a fixed width two's compliment)
	assert.Equal(t, "-ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff", iMaxVal)

	var iRead big.Int
	_, ok := iRead.SetString(iMaxVal.(string), 16)
	assert.True(t, ok)

}
func TestTooLargeInteger(t *testing.T) {

	var iMax FFBigInt
	_ = iMax.Int().Exp(big.NewInt(2), big.NewInt(256), nil)
	iMax.Int().Neg(iMax.Int())
	_, err := iMax.Value()
	assert.Regexp(t, "FF00103", err)

}

func TestScanNil(t *testing.T) {

	var nilVal interface{}
	var i FFBigInt
	err := i.Scan(nilVal)
	assert.NoError(t, err)
	assert.Zero(t, i.Int().Int64())

}

func TestScanString(t *testing.T) {

	var i FFBigInt
	err := i.Scan("-feedbeef")
	assert.NoError(t, err)
	assert.Equal(t, int64(-4276993775), i.Int().Int64())

}

func TestScanEmptyString(t *testing.T) {

	var i FFBigInt
	err := i.Scan("")
	assert.NoError(t, err)
	assert.Zero(t, i.Int().Int64())

}

func TestScanBadString(t *testing.T) {

	var i FFBigInt
	err := i.Scan("!hex")
	assert.Regexp(t, "FF00105", err)

}

func TestScanBadType(t *testing.T) {

	var i FFBigInt
	err := i.Scan(123456)
	assert.Regexp(t, "FF00105", err)

}

func TestEquals(t *testing.T) {

	var pi1, pi2 *FFBigInt
	assert.True(t, pi1.Equals(pi2))

	var i1 FFBigInt
	i1.Int().Set(big.NewInt(1))

	assert.False(t, i1.Equals(pi2))
	assert.False(t, pi2.Equals(&i1))

	var i2 FFBigInt
	i2.Int().Set(big.NewInt(1))

	assert.True(t, i1.Equals(&i2))
	assert.True(t, i2.Equals(&i1))

}

func TestNewBigInt(t *testing.T) {

	n := NewFFBigInt(10)
	assert.Equal(t, int64(10), n.Int().Int64())

}

func TestBigIntInt64(t *testing.T) {
	var n *FFBigInt
	assert.Equal(t, int64(0), n.Int64())
	n = NewFFBigInt(10)
	assert.Equal(t, int64(10), n.Int64())
}

func TestBigIntUint64(t *testing.T) {
	var n *FFBigInt
	assert.Equal(t, uint64(0), n.Uint64())
	n = NewFFBigInt(10)
	assert.Equal(t, uint64(10), n.Uint64())
	n = NewFFBigInt(-1)
	assert.Equal(t, uint64(0), n.Uint64())
}
