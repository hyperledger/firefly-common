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

package fftypes

import (
	"context"
	"crypto/sha256"
	"database/sql/driver"
	"encoding/json"
	"math/big"
	"strconv"
	"strings"

	"github.com/hyperledger/firefly-common/pkg/i18n"
	"github.com/hyperledger/firefly-common/pkg/log"
)

// JSONObject is a holder of a hash, that can be used to correlate onchain data with off-chain data.
type JSONObject map[string]interface{}

// Scan implements sql.Scanner
func (jd *JSONObject) Scan(src interface{}) error {
	switch src := src.(type) {
	case nil:
		return nil

	case string:
		if src == "" {
			return nil
		}
		return json.Unmarshal([]byte(src), &jd)

	case []byte:
		return json.Unmarshal(src, &jd)

	default:
		return i18n.NewError(context.Background(), i18n.MsgTypeRestoreFailed, src, jd)
	}

}

func (jd JSONObject) GetString(key string) string {
	s, _ := jd.GetStringOk(key)
	return s
}

func (jd JSONObject) GetInteger(key string) *big.Int {
	s := jd.GetString(key)
	if s == "" {
		log.L(context.Background()).Tracef("Int value unset for key '%s'", key)
		return big.NewInt(0)
	}
	i, ok := big.NewInt(0).SetString(s, 0)
	if !ok {
		log.L(context.Background()).Errorf("Invalid int value '%+v' for key '%s'", s, key)
		return big.NewInt(0)
	}
	return i
}

func (jd JSONObject) GetInt64(key string) int64 {
	return jd.GetInteger(key).Int64()
}

func (jd JSONObject) GetBool(key string) bool {
	vInterface := jd[key]
	switch vt := vInterface.(type) {
	case string:
		return strings.EqualFold(vt, "true")
	case bool:
		return vt
	default:
		return false
	}
}

func (jd JSONObject) GetStringOk(key string) (string, bool) {
	vInterface := jd[key]
	switch vt := vInterface.(type) {
	case string:
		return vt, true
	case bool:
		return strconv.FormatBool(vt), true
	case float32:
		return strconv.FormatFloat(float64(vt), 'f', -1, 64), true
	case float64:
		return strconv.FormatFloat(vt, 'f', -1, 64), true
	case int:
		return strconv.FormatInt(int64(vt), 10), true
	case int8:
		return strconv.FormatInt(int64(vt), 10), true
	case int16:
		return strconv.FormatInt(int64(vt), 10), true
	case int32:
		return strconv.FormatInt(int64(vt), 10), true
	case int64:
		return strconv.FormatInt(vt, 10), true
	case uint:
		return strconv.FormatUint(uint64(vt), 10), true
	case uint8:
		return strconv.FormatInt(int64(vt), 10), true
	case uint16:
		return strconv.FormatInt(int64(vt), 10), true
	case uint32:
		return strconv.FormatInt(int64(vt), 10), true
	case uint64:
		return strconv.FormatUint(vt, 10), true
	case nil:
		return "", false // no need to log for nil
	default:
		log.L(context.Background()).Errorf("Invalid string value '%+v' for key '%s'", vInterface, key)
		return "", false
	}
}

func (jd JSONObject) GetObject(key string) JSONObject {
	ob, _ := jd.GetObjectOk(key)
	return ob
}

func (jd JSONObject) GetObjectOk(key string) (JSONObject, bool) {
	vInterface, ok := jd[key]
	if ok && vInterface != nil {
		vInterface := jd[key]
		switch vMap := vInterface.(type) {
		case map[string]interface{}:
			return JSONObject(vMap), true
		case JSONObject:
			return vMap, true
		default:
			log.L(context.Background()).Errorf("Invalid object value '%+v' for key '%s'", vInterface, key)
			return JSONObject{}, false // Ensures a non-nil return
		}
	}
	return JSONObject{}, false // Ensures a non-nil return
}

func ToJSONObjectArray(unknown interface{}) (JSONObjectArray, bool) {
	vMap, ok := unknown.([]interface{})
	joa := make(JSONObjectArray, len(vMap))
	if !ok {
		joa, ok = unknown.(JSONObjectArray) // Case that we're passed a JSONObjectArray directly
	}
	for i, joi := range vMap {
		jo, childOK := joi.(map[string]interface{})
		if childOK {
			joa[i] = JSONObject(jo)
		}
		ok = ok && childOK
	}
	return joa, ok
}

func ToStringArray(unknown interface{}) ([]string, bool) {
	vArray, ok := unknown.([]interface{})
	joa := make([]string, len(vArray))
	if !ok {
		joa, ok = unknown.([]string) // Case that we're passed a []string directly
	}
	for i, joi := range vArray {
		jo, childOK := joi.(string)
		if childOK {
			joa[i] = jo
		}
		ok = ok && childOK
	}
	return joa, ok
}

func (jd JSONObject) GetObjectArray(key string) JSONObjectArray {
	oa, _ := jd.GetObjectArrayOk(key)
	return oa
}

func (jd JSONObject) GetObjectArrayOk(key string) (JSONObjectArray, bool) {
	vInterface, ok := jd[key]
	if ok && vInterface != nil {
		return ToJSONObjectArray(vInterface)
	}
	return JSONObjectArray{}, false // Ensures a non-nil return
}

func (jd JSONObject) GetStringArray(key string) []string {
	sa, _ := jd.GetStringArrayOk(key)
	return sa
}

func (jd JSONObject) GetStringArrayOk(key string) ([]string, bool) {
	vInterface, ok := jd[key]
	if ok && vInterface != nil {
		return ToStringArray(vInterface)
	}
	log.L(context.Background()).Errorf("Invalid string array value '%+v' for key '%s'", vInterface, key)
	return []string{}, false // Ensures a non-nil return
}

// Value implements sql.Valuer
func (jd JSONObject) Value() (driver.Value, error) {
	if jd == nil {
		return nil, nil
	}
	b, err := json.Marshal(&jd)
	if err != nil {
		return nil, err
	}
	return string(b), err
}

func (jd JSONObject) String() string {
	b, _ := json.Marshal(&jd)
	return string(b)
}

func (jd JSONObject) Hash(jsonDesc string) (*Bytes32, error) {
	b, err := json.Marshal(&jd)
	if err != nil {
		return nil, i18n.NewError(context.Background(), i18n.MsgJSONObjectParseFailed, jsonDesc)
	}
	var b32 Bytes32 = sha256.Sum256(b)
	return &b32, nil
}

func (jd JSONObject) DeepCopy() JSONObject {
	return deepCopyMap(jd)
}
