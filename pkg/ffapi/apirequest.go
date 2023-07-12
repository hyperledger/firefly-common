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

package ffapi

import (
	"net/http"
	"reflect"
)

type APIRequest struct {
	Req             *http.Request
	QP              map[string]string
	PP              map[string]string
	FP              map[string]string
	Filter          AndFilter
	Input           interface{}
	Part            *Multipart
	SuccessStatus   int
	ResponseHeaders http.Header
	AlwaysPaginate  bool
}

// FilterResult is a helper to transform a filter result into a REST API standard payload
func (r *APIRequest) FilterResult(items interface{}, res *FilterResult, err error) (interface{}, error) {
	itemsVal := reflect.ValueOf(items)
	if itemsVal.Kind() == reflect.Slice && (r.AlwaysPaginate || (res != nil && res.TotalCount != nil)) {
		response := &FilterResultsWithCount{
			Items: items,
		}
		if res != nil && res.TotalCount != nil {
			response.Total = *res.TotalCount
		}
		if itemsVal.Kind() == reflect.Slice {
			response.Count = int64(itemsVal.Len())
		}
		return response, err
	}
	return items, err

}
