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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFilterResultWithCount(t *testing.T) {
	r := &APIRequest{}
	ten := int64(10)
	f, err := r.FilterResult([]string{"test"}, &FilterResult{
		TotalCount: &ten,
	}, nil)
	assert.NoError(t, err)
	assert.Equal(t, &FilterResultsWithCount{
		Count: 1,
		Total: &ten,
		Items: []string{"test"},
	}, f)
}

func TestFilterResultPlain(t *testing.T) {
	r := &APIRequest{}
	f, err := r.FilterResult([]string{"test"}, &FilterResult{}, nil)
	assert.NoError(t, err)
	assert.Equal(t, []string{"test"}, f)
}

func TestFilterResultWithCountNoTotal(t *testing.T) {
	r := &APIRequest{
		AlwaysPaginate: true,
	}
	f, err := r.FilterResult([]string{"test"}, &FilterResult{}, nil)
	assert.NoError(t, err)
	assert.Equal(t, &FilterResultsWithCount{
		Count: 1,
		Items: []string{"test"},
	}, f)
}

func TestFilterResultAlwyasPaginateNoSlice(t *testing.T) {
	r := &APIRequest{
		AlwaysPaginate: true,
	}
	f, err := r.FilterResult("test", &FilterResult{}, nil)
	assert.NoError(t, err)
	assert.Equal(t, "test", f)
}

func TestFilterResultAlwaysPaginateNoFilterResult(t *testing.T) {
	r := &APIRequest{
		AlwaysPaginate: true,
	}
	f, err := r.FilterResult([]string{"test"}, nil, nil)
	assert.NoError(t, err)
	assert.Equal(t, &FilterResultsWithCount{
		Count: 1,
		Items: []string{"test"},
	}, f)
}

func TestItemsResult(t *testing.T) {
	f, err := ItemsResult([]string{"item1"}, &FilterResult{}, nil)
	assert.NoError(t, err)
	assert.Equal(t, []string{"item1"}, f.Items)
	assert.Equal(t, 1, f.Count)
	assert.Nil(t, f.Total)

	ten := int64(10)
	f, err = ItemsResult([]string{"item1"}, &FilterResult{
		TotalCount: &ten,
	}, nil)
	assert.NoError(t, err)
	assert.Equal(t, []string{"item1"}, f.Items)
	assert.Equal(t, 1, f.Count)
	assert.Equal(t, int64(10), *f.Total)
}
