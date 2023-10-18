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

package eventstreams

import (
	"context"
	"testing"

	"github.com/hyperledger/firefly-common/pkg/fftls"
	"github.com/stretchr/testify/assert"
)

func TestNewManagerFailMissingDefault(t *testing.T) {

	_, err := NewEventStreamManager[testConfigType](context.Background(), &Config{}, &mockEventSource{})
	assert.Regexp(t, "FF00217", err)

}

type mockEventSource struct {
	validate func(ctx context.Context, conf *testConfigType) error
	run      func(ctx context.Context, es *EventStreamSpec[testConfigType], checkpointSequenceId string, deliver Deliver) error
}

func (mes *mockEventSource) Run(ctx context.Context, es *EventStreamSpec[testConfigType], checkpointSequenceId string, deliver Deliver) error {
	return mes.run(ctx, es, checkpointSequenceId, deliver)
}

func (mes *mockEventSource) Validate(ctx context.Context, conf *testConfigType) error {
	return mes.validate(ctx, conf)
}

func TestNewManagerFailBadTLS(t *testing.T) {

	_, err := NewEventStreamManager[testConfigType](context.Background(), &Config{
		TLSConfigs: map[string]*fftls.Config{
			"tls0": {
				Enabled: true,
				CAFile:  t.TempDir(),
			},
		},
	}, &mockEventSource{})
	assert.Regexp(t, "FF00153", err)

}
