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

package fftypes

import (
	"github.com/aidarkhanov/nanoid"
)

const (
	// ShortIDlphabet is designed for easy double-click select
	ShortIDlphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz"
)

func ShortID() string {
	return nanoid.Must(nanoid.Generate(ShortIDlphabet, 8))
}

func SafeHashCompare(h1 *Bytes32, h2 *Bytes32) bool {
	if h1 == nil && h2 == nil {
		return true
	}
	if h1 == nil && h2 != nil || h2 == nil && h1 != nil {
		return false
	}
	return *h1 == *h2
}
