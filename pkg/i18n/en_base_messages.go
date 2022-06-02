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
	APISuccessResponse    = ffm("api.success", "Success")
	APIRequestTimeoutDesc = ffm("api.requestTimeout", "Server-side request timeout (milliseconds, or set a custom suffix like 10s)")
)
