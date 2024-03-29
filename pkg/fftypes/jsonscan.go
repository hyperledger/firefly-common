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
	"context"
	"database/sql/driver"
	"encoding/json"

	"github.com/hyperledger/firefly-common/pkg/i18n"
)

func JSONScan(src, target interface{}) error {
	switch src := src.(type) {
	case string:
		return json.Unmarshal([]byte(src), target)
	case []byte:
		return json.Unmarshal(src, target)
	case nil:
		return nil
	default:
		return i18n.NewError(context.Background(), i18n.MsgScanTypeMismatch, src)
	}
}

func JSONValue(target interface{}) (driver.Value, error) {
	if IsNil(target) {
		return nil, nil
	}
	return json.Marshal(target)
}
