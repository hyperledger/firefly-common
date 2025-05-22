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

func deepCopyMap(original map[string]interface{}) map[string]interface{} {
	if original == nil {
		return nil
	}
	co := make(map[string]interface{}, len(original))
	for key, value := range original {
		switch v := value.(type) {
		case map[string]interface{}:
			co[key] = deepCopyMap(v)
		case []interface{}:
			co[key] = deepCopySlice(v)
		default:
			co[key] = v
		}
	}
	return co
}

func deepCopySlice(original []interface{}) []interface{} {
	if original == nil {
		return nil
	}
	co := make([]interface{}, len(original))
	for i, value := range original {
		switch v := value.(type) {
		case map[string]interface{}:
			co[i] = deepCopyMap(v)
		case []interface{}:
			co[i] = deepCopySlice(v)
		default:
			co[i] = v
		}
	}
	return co
}
