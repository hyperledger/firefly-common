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
	_ "embed"
	"net/http"
	"strings"
)

//go:embed fflogo16.png
var ffLogo16 []byte

//go:embed fflogo32.png
var ffLogo32 []byte

func favIconsHandler(logo16, logo32 []byte) http.HandlerFunc {
	return func(res http.ResponseWriter, req *http.Request) {
		res.Header().Set("content-type", "image/png")
		if strings.Contains(req.URL.Path, "16") {
			_, _ = res.Write(ffLogo16)
		} else {
			_, _ = res.Write(ffLogo32)
		}
	}
}
