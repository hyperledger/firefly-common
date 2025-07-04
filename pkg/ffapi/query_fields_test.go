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

package ffapi

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/hyperledger/firefly-common/pkg/fftypes"
	"github.com/stretchr/testify/assert"
)

func TestNullField(t *testing.T) {

	f := nullField{}
	v, err := f.Value()
	assert.NoError(t, err)
	assert.Nil(t, v)

	err = f.Scan("anything")
	assert.NoError(t, err)
	v, err = f.Value()
	assert.NoError(t, err)
	assert.Nil(t, v)

	assert.Equal(t, "null", f.String())
}

func TestStringField(t *testing.T) {

	fd := &StringField{}
	assert.NotEmpty(t, fd.Description())
	f := stringField{}

	err := f.Scan("test")
	assert.NoError(t, err)
	v, err := f.Value()
	assert.NoError(t, err)
	assert.Equal(t, "test", v)

	err = f.Scan(nil)
	assert.NoError(t, err)
	v, err = f.Value()
	assert.NoError(t, err)
	assert.Equal(t, "", v)

	err = f.Scan(fftypes.FFEnum("12345"))
	assert.NoError(t, err)
	v, err = f.Value()
	assert.NoError(t, err)
	assert.Equal(t, "12345", v)

}

func TestUUIDField(t *testing.T) {

	fd := &UUIDField{}
	assert.NotEmpty(t, fd.Description())
	f := uuidField{}

	err := f.Scan("")
	assert.NoError(t, err)
	v, err := f.Value()
	assert.NoError(t, err)
	assert.Nil(t, v)

	u1 := fftypes.NewUUID()
	err = f.Scan(u1.String())
	assert.NoError(t, err)
	v, err = f.Value()
	assert.NoError(t, err)
	assert.Equal(t, v, u1.String())

	err = f.Scan(nil)
	assert.NoError(t, err)
	v, err = f.Value()
	assert.NoError(t, err)
	assert.Nil(t, v)

}

func TestBytes32Field(t *testing.T) {

	fd := &Bytes32Field{}
	assert.NotEmpty(t, fd.Description())
	f := bytes32Field{}

	err := f.Scan("")
	assert.NoError(t, err)
	v, err := f.Value()
	assert.NoError(t, err)
	assert.Nil(t, v)

	b1 := fftypes.NewRandB32()
	err = f.Scan(b1.String())
	assert.NoError(t, err)
	v, err = f.Value()
	assert.NoError(t, err)
	assert.Equal(t, v, b1.String())

	err = f.Scan(nil)
	assert.NoError(t, err)
	v, err = f.Value()
	assert.NoError(t, err)
	assert.Nil(t, v)

}

func TestInt64Field(t *testing.T) {

	fd := &Int64Field{}
	assert.NotEmpty(t, fd.Description())
	f := int64Field{}

	err := f.Scan("12345")
	assert.NoError(t, err)
	v, err := f.Value()
	assert.NoError(t, err)
	assert.Equal(t, int64(12345), v)

	err = f.Scan(nil)
	assert.NoError(t, err)
	v, err = f.Value()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), v)

	err = f.Scan(uint64(math.MaxInt64 + 1))
	assert.Regexp(t, "FF00105", err)

	err = f.Scan(uint(math.MaxInt64 + 1))
	assert.Regexp(t, "FF00105", err)

	err = f.Scan(fmt.Sprintf("%d", uint64(math.MaxInt64+1)))
	assert.Regexp(t, "FF00105", err)

}

func TestBigIntField(t *testing.T) {

	fd := &BigIntField{}
	assert.NotEmpty(t, fd.Description())
	f := bigIntField{}
	assert.NotNil(t, fd.GetSerialization())
	assert.False(t, fd.FilterAsString())
	assert.Equal(t, "BigInteger", fd.Description())

	for _, v := range []interface{}{
		10,
		int32(10),
		int64(10),
		uint(10),
		uint32(10),
		uint64(10),
		"10",
		fftypes.NewFFBigInt(10),
		*fftypes.NewFFBigInt(10),
		fftypes.NewFFBigInt(10).Int(),
		*fftypes.NewFFBigInt(10).Int(),
	} {
		err := f.Scan(v)
		assert.NoError(t, err)
		v, err := f.Value()
		assert.NoError(t, err)
		assert.Equal(t, "000000000000000000000000000000000000000000000000000000000000000a", v)
		assert.Equal(t, "10", f.String())
	}

	err := f.Scan(nil)
	assert.NoError(t, err)
	v, err := f.Value()
	assert.NoError(t, err)
	assert.Equal(t, "0000000000000000000000000000000000000000000000000000000000000000", v)

	err = f.Scan("lobster")
	assert.Regexp(t, "FF00105", err)

	err = f.Scan(false)
	assert.Regexp(t, "FF00105", err)

}

func TestTimeField(t *testing.T) {

	fd := &TimeField{}
	assert.NotEmpty(t, fd.Description())
	f := timeField{}

	now := time.Now()
	err := f.Scan(now.Format(time.RFC3339Nano))
	assert.NoError(t, err)
	v, err := f.Value()
	assert.NoError(t, err)
	assert.Equal(t, v, now.UnixNano())

	err = f.Scan(nil)
	assert.NoError(t, err)
	v, err = f.Value()
	assert.NoError(t, err)
	assert.Nil(t, v)

}

func TestJSONField(t *testing.T) {

	fd := &JSONField{}
	assert.NotEmpty(t, fd.Description())
	f := jsonField{}

	err := f.Scan("{}")
	assert.NoError(t, err)
	v, err := f.Value()
	assert.NoError(t, err)
	assert.Equal(t, v, []byte("{}"))

	err = f.Scan(fftypes.JSONAnyPtr("{}"))
	assert.NoError(t, err)

	err = f.Scan(nil)
	assert.NoError(t, err)
	v, err = f.Value()
	assert.NoError(t, err)
	assert.Nil(t, v)

}

func TestBoolField(t *testing.T) {

	fd := &BoolField{}
	assert.NotEmpty(t, fd.Description())
	f := boolField{}

	err := f.Scan("true")
	assert.NoError(t, err)
	v, err := f.Value()
	assert.NoError(t, err)
	assert.True(t, v.(bool))

	err = f.Scan(nil)
	assert.NoError(t, err)
	v, err = f.Value()
	assert.NoError(t, err)
	assert.False(t, v.(bool))

}

func TestFFStringArrayField(t *testing.T) {

	fd := &FFStringArrayField{}
	assert.NotEmpty(t, fd.Description())
	f := ffNameArrayField{}

	err := f.Scan("a,b")
	assert.NoError(t, err)
	v, err := f.Value()
	assert.NoError(t, err)
	assert.Equal(t, v, "a,b")

	err = f.Scan(nil)
	assert.NoError(t, err)
	v, err = f.Value()
	assert.NoError(t, err)
	assert.Equal(t, "", v)

}
