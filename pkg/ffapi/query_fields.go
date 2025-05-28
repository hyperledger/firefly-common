// Copyright © 2024 Kaleido, Inc.
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
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"reflect"
	"strconv"
	"strings"

	"github.com/hyperledger/firefly-common/pkg/fftypes"
	"github.com/hyperledger/firefly-common/pkg/i18n"
)

// QueryFactory creates a filter builder in the given context, and contains the rules on
// which fields can be used by the builder (and how they are serialized)
type QueryFactory interface {
	NewFilter(ctx context.Context) FilterBuilder
	NewFilterLimit(ctx context.Context, defLimit uint64) FilterBuilder
	NewUpdate(ctx context.Context) UpdateBuilder
	Clone() ClonedQueryFactory
}

type ClonedQueryFactory interface {
	QueryFactory
	AddField(n string, f Field)
}

type FieldMod int

const (
	FieldModLower FieldMod = iota
)

// HasFieldMods can be set on a QueryField to do special things that a DB might support - like lowercase index filtering
type HasFieldMods interface {
	FieldMods() []FieldMod
}

type QueryFields map[string]Field

func (qf QueryFields) NewFilterLimit(ctx context.Context, defLimit uint64) FilterBuilder {
	return &filterBuilder{
		ctx:         ctx,
		queryFields: qf,
		limit:       defLimit,
	}
}

func (qf QueryFields) NewFilter(ctx context.Context) FilterBuilder {
	return qf.NewFilterLimit(ctx, 0)
}

func (qf QueryFields) NewUpdate(ctx context.Context) UpdateBuilder {
	return &updateBuilder{
		ctx:         ctx,
		queryFields: qf,
	}
}

func (qf QueryFields) AddField(n string, f Field) {
	qf[n] = f
}

func (qf QueryFields) Clone() ClonedQueryFactory {
	qf2 := make(QueryFields, len(qf))
	for n, f := range qf {
		qf2[n] = f
	}
	return qf2
}

// FieldSerialization - we stand on the shoulders of the well adopted SQL serialization interface here to help us define what
// string<->value looks like, even though this plugin interface is not tightly coupled to SQL.
type FieldSerialization interface {
	driver.Valuer
	sql.Scanner // Implementations can assume the value is ALWAYS a string
}

type Field interface {
	GetSerialization() FieldSerialization
	Description() string
	FilterAsString() bool
}

// nullField is a special FieldSerialization used to represent nil in queries
type nullField struct{}

func (f *nullField) Scan(_ interface{}) error {
	return nil
}
func (f *nullField) Value() (driver.Value, error) { return nil, nil }
func (f *nullField) String() string               { return fftypes.NullString }

type StringField struct{}
type stringField struct {
	s     string
	lower bool
}

func (f *stringField) Scan(src interface{}) error {
	switch tv := src.(type) {
	case string:
		f.s = tv
	case int:
		f.s = strconv.FormatInt(int64(tv), 10)
	case int32:
		f.s = strconv.FormatInt(int64(tv), 10)
	case int64:
		f.s = strconv.FormatInt(tv, 10)
	case uint:
		f.s = strconv.FormatUint(uint64(tv), 10)
	case uint32:
		f.s = strconv.FormatUint(uint64(tv), 10)
	case uint64:
		f.s = strconv.FormatUint(tv, 10)
	case *fftypes.UUID:
		if tv != nil {
			f.s = tv.String()
		}
	case fftypes.UUID:
		f.s = tv.String()
	case *fftypes.Bytes32:
		if tv != nil {
			f.s = tv.String()
		}
	case fftypes.Bytes32:
		f.s = tv.String()
	case nil:
		f.s = ""
	default:
		if reflect.TypeOf(tv).Kind() == reflect.String {
			// This is helpful for status enums
			f.s = reflect.ValueOf(tv).String()
		} else {
			return i18n.NewError(context.Background(), i18n.MsgTypeRestoreFailed, src, f.s)
		}
	}
	return nil
}
func (f *stringField) Value() (driver.Value, error) {
	if f.lower {
		return strings.ToLower(f.s), nil
	}
	return f.s, nil
}
func (f *stringField) String() string                       { return f.s }
func (f *StringField) GetSerialization() FieldSerialization { return &stringField{} }
func (f *StringField) FilterAsString() bool                 { return true }
func (f *StringField) Description() string                  { return "String" }

type StringFieldLower struct {
	StringField
}

func (f *StringFieldLower) GetSerialization() FieldSerialization { return &stringField{lower: true} }
func (f *StringFieldLower) FilterAsString() bool                 { return true }
func (f *StringFieldLower) FieldMods() []FieldMod                { return []FieldMod{FieldModLower} }

type UUIDField struct{}
type uuidField struct{ u *fftypes.UUID }

func (f *uuidField) Scan(src interface{}) (err error) {
	switch tv := src.(type) {
	case string:
		if tv == "" {
			f.u = nil
			return nil
		}
		f.u, err = fftypes.ParseUUID(context.Background(), tv)
		return err
	case *fftypes.UUID:
		f.u = tv
	case fftypes.UUID:
		u := tv
		f.u = &u
	case *fftypes.Bytes32:
		if tv == nil {
			f.u = nil
			return nil
		}
		var u fftypes.UUID
		copy(u[:], tv[0:16])
		f.u = &u
	case fftypes.Bytes32:
		var u fftypes.UUID
		copy(u[:], tv[0:16])
		f.u = &u
	case nil:
		f.u = nil
	default:
		return i18n.NewError(context.Background(), i18n.MsgTypeRestoreFailed, src, f.u)
	}
	return nil
}
func (f *uuidField) Value() (driver.Value, error)         { return f.u.Value() }
func (f *uuidField) String() string                       { return fmt.Sprintf("%v", f.u) }
func (f *UUIDField) GetSerialization() FieldSerialization { return &uuidField{} }
func (f *UUIDField) FilterAsString() bool                 { return true }
func (f *UUIDField) Description() string                  { return "UUID" }

type Bytes32Field struct{}
type bytes32Field struct{ b32 *fftypes.Bytes32 }

func (f *bytes32Field) Scan(src interface{}) (err error) {
	switch tv := src.(type) {
	case string:
		if tv == "" {
			f.b32 = nil
			return nil
		}
		f.b32, err = fftypes.ParseBytes32(context.Background(), tv)
		return err
	case *fftypes.Bytes32:
		f.b32 = tv
	case fftypes.Bytes32:
		b32 := tv
		f.b32 = &b32
	case nil:
		f.b32 = nil
	default:
		return i18n.NewError(context.Background(), i18n.MsgTypeRestoreFailed, src, f.b32)
	}
	return nil
}
func (f *bytes32Field) Value() (driver.Value, error)         { return f.b32.Value() }
func (f *bytes32Field) String() string                       { return fmt.Sprintf("%v", f.b32) }
func (f *Bytes32Field) GetSerialization() FieldSerialization { return &bytes32Field{} }
func (f *Bytes32Field) FilterAsString() bool                 { return true }
func (f *Bytes32Field) Description() string                  { return "Byte-Array" }

type Int64Field struct{}
type int64Field struct{ i int64 }

func (f *int64Field) Scan(src interface{}) (err error) {
	switch tv := src.(type) {
	case int:
		f.i = int64(tv)
	case int32:
		f.i = int64(tv)
	case int64:
		f.i = tv
	case uint:
		if tv > math.MaxInt64 {
			return i18n.NewError(context.Background(), i18n.MsgTypeRestoreFailed, src, f.i)
		}
		f.i = int64(tv)
	case uint32:
		f.i = int64(tv)
	case uint64:
		if tv > math.MaxInt64 {
			return i18n.NewError(context.Background(), i18n.MsgTypeRestoreFailed, src, f.i)
		}
		f.i = int64(tv)
	case string:
		f.i, err = strconv.ParseInt(src.(string), 10, 64)
		if err != nil {
			return i18n.WrapError(context.Background(), err, i18n.MsgTypeRestoreFailed, src, int64(0))
		}
	case nil:
		f.i = 0
	default:
		return i18n.NewError(context.Background(), i18n.MsgTypeRestoreFailed, src, f.i)
	}
	return nil
}
func (f *int64Field) Value() (driver.Value, error)         { return f.i, nil }
func (f *int64Field) String() string                       { return fmt.Sprintf("%d", f.i) }
func (f *Int64Field) GetSerialization() FieldSerialization { return &int64Field{} }
func (f *Int64Field) FilterAsString() bool                 { return false }
func (f *Int64Field) Description() string                  { return "Integer" }

type BigIntField struct{}
type bigIntField struct{ i *fftypes.FFBigInt }

func (f *bigIntField) Scan(src interface{}) (err error) {
	switch tv := src.(type) {
	case int:
		f.i = fftypes.NewFFBigInt(int64(tv))
	case int32:
		f.i = fftypes.NewFFBigInt(int64(tv))
	case int64:
		f.i = fftypes.NewFFBigInt(tv)
	case uint:
		f.i = (*fftypes.FFBigInt)(new(big.Int).SetUint64(uint64(tv)))
	case uint32:
		f.i = fftypes.NewFFBigInt(int64(tv))
	case uint64:
		f.i = (*fftypes.FFBigInt)(new(big.Int).SetUint64(tv))
	case fftypes.FFBigInt:
		i := tv
		f.i = &i
	case *fftypes.FFBigInt:
		i := *tv
		f.i = &i
	case big.Int:
		i := fftypes.FFBigInt(tv)
		f.i = &i
	case *big.Int:
		i := fftypes.FFBigInt(*tv)
		f.i = &i
	case string:
		i := new(big.Int)
		i, ok := i.SetString(tv, 0)
		if !ok {
			return i18n.NewError(context.Background(), i18n.MsgTypeRestoreFailed, src, int64(0))
		}
		f.i = (*fftypes.FFBigInt)(i)
	case nil:
		f.i = fftypes.NewFFBigInt(0)
	default:
		return i18n.NewError(context.Background(), i18n.MsgTypeRestoreFailed, src, f.i)
	}
	return nil
}
func (f *bigIntField) Value() (driver.Value, error)         { return f.i.Value() } // Hex string in DB is a padded hex string that's text sortable
func (f *bigIntField) String() string                       { return f.i.Int().Text(10) }
func (f *BigIntField) GetSerialization() FieldSerialization { return &bigIntField{} }
func (f *BigIntField) FilterAsString() bool                 { return false }
func (f *BigIntField) Description() string                  { return "BigInteger" }

type TimeField struct{}
type timeField struct{ t *fftypes.FFTime }

func (f *timeField) Scan(src interface{}) (err error) {
	switch tv := src.(type) {
	case int:
		f.t = fftypes.UnixTime(int64(tv))
	case int64:
		f.t = fftypes.UnixTime(tv)
	case string:
		f.t, err = fftypes.ParseTimeString(tv)
		return err
	case fftypes.FFTime:
		f.t = &tv
		return nil
	case *fftypes.FFTime:
		f.t = tv
		return nil
	case nil:
		f.t = nil
	default:
		return i18n.NewError(context.Background(), i18n.MsgTypeRestoreFailed, src, f.t)
	}
	return nil
}
func (f *timeField) Value() (driver.Value, error) {
	if f.t == nil {
		return nil, nil
	}
	return f.t.UnixNano(), nil
}
func (f *timeField) String() string                       { return fmt.Sprintf("%v", f.t) }
func (f *TimeField) GetSerialization() FieldSerialization { return &timeField{} }
func (f *TimeField) FilterAsString() bool                 { return false }
func (f *TimeField) Description() string                  { return "Date-time" }

type JSONField struct{}
type jsonField struct{ b []byte }

func (f *jsonField) Scan(src interface{}) (err error) {
	switch tv := src.(type) {
	case string:
		f.b = []byte(tv)
	case []byte:
		f.b = tv
	case fftypes.JSONObject:
		f.b, err = json.Marshal(tv)
	case nil:
		f.b = nil
	case *fftypes.JSONAny:
		f.b = tv.Bytes()
	default:
		return i18n.NewError(context.Background(), i18n.MsgTypeRestoreFailed, src, f.b)
	}
	return err
}
func (f *jsonField) Value() (driver.Value, error)         { return f.b, nil }
func (f *jsonField) String() string                       { return string(f.b) }
func (f *JSONField) GetSerialization() FieldSerialization { return &jsonField{} }
func (f *JSONField) FilterAsString() bool                 { return true }
func (f *JSONField) Description() string                  { return "JSON-blob" }

type FFStringArrayField struct{}
type ffNameArrayField struct{ na fftypes.FFStringArray }

func (f *ffNameArrayField) Scan(src interface{}) (err error) {
	return f.na.Scan(src)
}
func (f *ffNameArrayField) Value() (driver.Value, error)           { return f.na.String(), nil }
func (f *ffNameArrayField) String() string                         { return f.na.String() }
func (f *FFStringArrayField) GetSerialization() FieldSerialization { return &ffNameArrayField{} }
func (f *FFStringArrayField) FilterAsString() bool                 { return true }
func (f *FFStringArrayField) Description() string                  { return "String-array" }

type BoolField struct{}
type boolField struct{ b bool }

func (f *boolField) Scan(src interface{}) (err error) {
	switch tv := src.(type) {
	case int:
		f.b = tv != 0
	case int32:
		f.b = tv != 0
	case int64:
		f.b = tv != 0
	case uint:
		f.b = tv != 0
	case uint32:
		f.b = tv != 0
	case uint64:
		f.b = tv != 0
	case bool:
		f.b = tv
	case string:
		f.b = strings.EqualFold(tv, "true")
	case nil:
		f.b = false
	default:
		return i18n.NewError(context.Background(), i18n.MsgTypeRestoreFailed, src, f.b)
	}
	return nil
}
func (f *boolField) Value() (driver.Value, error)         { return f.b, nil }
func (f *boolField) String() string                       { return fmt.Sprintf("%t", f.b) }
func (f *BoolField) GetSerialization() FieldSerialization { return &boolField{} }
func (f *BoolField) FilterAsString() bool                 { return false }
func (f *BoolField) Description() string                  { return "Boolean" }
