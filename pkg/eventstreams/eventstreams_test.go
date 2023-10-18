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

package eventstreams

import (
	"testing"

	"github.com/hyperledger/firefly-common/pkg/dbsql"
	"github.com/hyperledger/firefly-common/pkg/fftypes"
	"github.com/stretchr/testify/assert"
)

type testConfigType struct {
	CustomField1 string `json:"field1"`
}

func TestEventStreamFields(t *testing.T) {

	es := &EventStreamSpec[testConfigType]{
		ResourceBase: dbsql.ResourceBase{
			ID: fftypes.NewUUID(),
		},
	}
	assert.Equal(t, es.ID.String(), es.GetID())
	t1 := fftypes.Now()
	es.SetCreated(t1)
	assert.Equal(t, t1, es.Created)
	t2 := fftypes.Now()
	es.SetUpdated(t2)
	assert.Equal(t, t2, es.Updated)
}

func TestWebSocketConfigSerialization(t *testing.T) {

	var wc *WebSocketConfig
	v, err := wc.Value()
	assert.Nil(t, v)
	assert.NoError(t, err)

	wc = &WebSocketConfig{
		DistributionMode: &DistributionModeBroadcast,
	}
	v, err = wc.Value()
	assert.NotNil(t, v)
	assert.NoError(t, err)

	wc1 := &WebSocketConfig{}
	err = wc1.Scan(v)
	assert.NoError(t, err)
	assert.Equal(t, DistributionModeBroadcast, *wc1.DistributionMode)

	wc2 := &WebSocketConfig{}
	err = wc2.Scan(string(v.([]byte)))
	assert.NoError(t, err)
	assert.Equal(t, DistributionModeBroadcast, *wc1.DistributionMode)

	var wc3 *WebSocketConfig
	err = wc3.Scan(nil)
	assert.NoError(t, err)
	assert.Nil(t, wc3)

}

func TestWebhookConfigSerialization(t *testing.T) {

	var wc *WebhookConfig
	v, err := wc.Value()
	assert.Nil(t, v)
	assert.NoError(t, err)

	u := "http://example.com"
	wc = &WebhookConfig{
		URL: &u,
	}
	v, err = wc.Value()
	assert.NotNil(t, v)
	assert.NoError(t, err)

	wc1 := &WebhookConfig{}
	err = wc1.Scan(v)
	assert.NoError(t, err)
	assert.Equal(t, "http://example.com", *wc1.URL)

	wc2 := &WebhookConfig{}
	err = wc2.Scan(string(v.([]byte)))
	assert.NoError(t, err)
	assert.Equal(t, "http://example.com", *wc1.URL)

	var wc3 *WebhookConfig
	err = wc3.Scan(nil)
	assert.NoError(t, err)
	assert.Nil(t, wc3)

}
