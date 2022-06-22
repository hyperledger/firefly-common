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
	"crypto/sha256"
)

func TypeNamespaceNameTopicHash(objType string, ns string, name string) string {
	// Topic generation function for ordering anything with a type, namespace and name.
	// Means all messages racing for this name will be consistently ordered by all parties.
	h := sha256.New()
	h.Write([]byte(objType))
	h.Write([]byte(ns))
	h.Write([]byte(name))
	return HashResult(h).String()
}
