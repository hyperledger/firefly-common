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

package i18n

import (
	"golang.org/x/text/language"
)

var ffm = func(key, translation string) MessageKey {
	return FFM(language.AmericanEnglish, key, translation)
}

var (
	APISuccessResponse      = ffm("api.success", "Success")
	APIRequestTimeoutDesc   = ffm("api.requestTimeout", "Server-side request timeout (milliseconds, or set a custom suffix like 10s)")
	APIFilterParamDesc      = ffm("api.filterParam", "Data filter field. Prefixes supported: > >= < <= @ ^ ! !@ !^")
	APIFilterSortDesc       = ffm("api.filterSort", "Sort field. For multi-field sort use comma separated values (or multiple query values) with '-' prefix for descending")
	APIFilterAscendingDesc  = ffm("api.filterAscending", "Ascending sort order (overrides all fields in a multi-field sort)")
	APIFilterDescendingDesc = ffm("api.filterDescending", "Descending sort order (overrides all fields in a multi-field sort)")
	APIFilterSkipDesc       = ffm("api.filterSkip", "The number of records to skip (max: %d). Unsuitable for bulk operations")
	APIFilterLimitDesc      = ffm("api.filterLimit", "The maximum number of records to return (max: %d)")
	APIFilterCountDesc      = ffm("api.filterCount", "Return a total count as well as items (adds extra database processing)")
)
