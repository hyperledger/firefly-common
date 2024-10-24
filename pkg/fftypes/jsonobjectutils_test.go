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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDeepCopyMapNil(t *testing.T) {
	original := map[string]interface{}(nil)
	copy := deepCopyMap(original)
	assert.Nil(t, copy)
}

func TestDeepCopyMapEmpty(t *testing.T) {
	original := map[string]interface{}{}
	copy := deepCopyMap(original)
	assert.NotNil(t, copy)
	assert.Empty(t, copy)
}

func TestDeepCopyMapSimple(t *testing.T) {
	original := map[string]interface{}{
		"key1": "value1",
		"key2": 42,
	}
	copy := deepCopyMap(original)
	assert.Equal(t, original, copy)
}

func TestDeepCopyMapNestedMap(t *testing.T) {
	original := map[string]interface{}{
		"key1": map[string]interface{}{
			"nestedKey1": "nestedValue1",
		},
	}
	copy := deepCopyMap(original)
	assert.Equal(t, original, copy)
	assert.NotSame(t, original["key1"], copy["key1"])
}

func TestDeepCopyMapNestedSlice(t *testing.T) {
	original := map[string]interface{}{
		"key1": []interface{}{"value1", 42},
	}
	copy := deepCopyMap(original)
	assert.Equal(t, original, copy)
	assert.NotSame(t, original["key1"], copy["key1"])
}

func TestDeepCopyMapMixed(t *testing.T) {
	original := map[string]interface{}{
		"key1": "value1",
		"key2": map[string]interface{}{
			"nestedKey1": "nestedValue1",
		},
		"key3": []interface{}{"value1", 42},
	}
	copy := deepCopyMap(original)
	assert.Equal(t, original, copy)
	assert.NotSame(t, original["key2"], copy["key2"])
	assert.NotSame(t, original["key3"], copy["key3"])
}

func TestDeepCopySliceNil(t *testing.T) {
	original := []interface{}(nil)
	copy := deepCopySlice(original)
	assert.Nil(t, copy)
}

func TestDeepCopySliceEmpty(t *testing.T) {
	original := []interface{}{}
	copy := deepCopySlice(original)
	assert.NotNil(t, copy)
	assert.Empty(t, copy)
}

func TestDeepCopySliceSimple(t *testing.T) {
	original := []interface{}{"value1", 42}
	copy := deepCopySlice(original)
	assert.Equal(t, original, copy)
}

func TestDeepCopySliceNestedMap(t *testing.T) {
	original := []interface{}{
		map[string]interface{}{
			"nestedKey1": "nestedValue1",
		},
	}
	copy := deepCopySlice(original)
	assert.Equal(t, original, copy)
	assert.NotSame(t, original[0], copy[0])
}

func TestDeepCopySliceNestedSlice(t *testing.T) {
	original := []interface{}{
		[]interface{}{"value1", 42},
	}
	copy := deepCopySlice(original)
	assert.Equal(t, original, copy)
	assert.NotSame(t, original[0], copy[0])
}

func TestDeepCopySliceMixed(t *testing.T) {
	original := []interface{}{
		"value1",
		42,
		map[string]interface{}{
			"nestedKey1": "nestedValue1",
		},
		[]interface{}{"value2", 43},
	}
	copy := deepCopySlice(original)
	assert.Equal(t, original, copy)
	assert.NotSame(t, original[2], copy[2])
	assert.NotSame(t, original[3], copy[3])
}
