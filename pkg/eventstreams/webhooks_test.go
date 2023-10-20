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
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/hyperledger/firefly-common/pkg/ffapi"
	"github.com/hyperledger/firefly-common/pkg/ffresty"
	"github.com/hyperledger/firefly-common/pkg/fftls"
	"github.com/hyperledger/firefly-common/pkg/fftypes"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func newTestWebhooks(t *testing.T, whc *WebhookConfig, tweaks ...func()) *webhookAction[testESConfig, testData] {

	ctx, mgr, _, done := newMockESManager(t, func(mdb *mockPersistence) {
		mdb.eventStreams.On("GetMany", mock.Anything, mock.Anything).Return([]*EventStreamSpec[testESConfig]{}, &ffapi.FilterResult{}, nil)
		WebhookDefaultsConfig.Set(ffresty.HTTPConfigRequestTimeout, "1s")
		WebhookDefaultsConfig.SubSection("tls").Set(fftls.HTTPConfTLSInsecureSkipHostVerify, true)
		for _, tweak := range tweaks {
			tweak()
		}
	})
	done()

	assert.NoError(t, whc.validate(ctx, mgr.tlsConfigs))

	return mgr.newWebhookAction(context.Background(), whc)
}

func TestWebhooksConfigValidate(t *testing.T) {

	wc := &WebhookConfig{}
	assert.Regexp(t, "FF00216", wc.validate(context.Background(), nil))

	u := "http://test.example"
	wc.URL = &u
	assert.NoError(t, wc.validate(context.Background(), nil))

	tlsConfName := "wrong"
	wc = &WebhookConfig{
		URL:           &u,
		TLSConfigName: &tlsConfName,
	}
	assert.Regexp(t, "FF00223", wc.validate(context.Background(), nil))

}

func TestWebhooksBadHost(t *testing.T) {
	u := "http://www.sample.invalid/guaranteed-to-fail"
	wh := newTestWebhooks(t, &WebhookConfig{URL: &u})

	err := wh.AttemptDispatch(context.Background(), 0, &EventBatch[testData]{
		StreamID:    fftypes.NewUUID(),
		BatchNumber: 1,
		Events: []*Event[testData]{
			{Data: &testData{Field1: 12345}},
		},
	})
	assert.Regexp(t, "FF00218", err)
}

func TestWebhooksPrivateBlocked(t *testing.T) {
	u := "http://10.0.0.1/one-of-the-private-ranges"
	wh := newTestWebhooks(t, &WebhookConfig{URL: &u}, func() {
		RootConfig.Set(ConfigDisablePrivateIPs, true)
	})

	err := wh.AttemptDispatch(context.Background(), 0, &EventBatch[testData]{
		StreamID:    fftypes.NewUUID(),
		BatchNumber: 1,
		Events: []*Event[testData]{
			{Data: &testData{Field1: 12345}},
		},
	})
	assert.Regexp(t, "FF00220", err)
}

func TestWebhooksCustomHeaders403(t *testing.T) {

	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/test/path", r.URL.Path)
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "test-value", r.Header.Get("test-header"))
		var batch *EventBatch[testData]
		err := json.NewDecoder(r.Body).Decode(&batch)
		assert.NoError(t, err)
		assert.Equal(t, 12345, batch.Events[0].Data.Field1)
		w.WriteHeader(403)
	}))
	defer s.Close()

	u := fmt.Sprintf("http://%s/test/path", s.Listener.Addr())
	wh := newTestWebhooks(t, &WebhookConfig{URL: &u})
	wh.spec.Headers = map[string]string{
		"test-header": "test-value",
	}

	done := make(chan struct{})
	go func() {
		err := wh.AttemptDispatch(context.Background(), 0, &EventBatch[testData]{
			StreamID:    fftypes.NewUUID(),
			BatchNumber: 1,
			Events: []*Event[testData]{
				{Data: &testData{Field1: 12345}},
			},
		})
		assert.Regexp(t, "FF00221.*403", err)
		close(done)
	}()
	<-done
}

func TestWebhooksCustomHeadersConnectFail(t *testing.T) {

	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	s.Close()

	u := fmt.Sprintf("http://%s/test/path", s.Listener.Addr())
	wh := newTestWebhooks(t, &WebhookConfig{URL: &u})

	done := make(chan struct{})
	go func() {
		err := wh.AttemptDispatch(context.Background(), 0, &EventBatch[testData]{
			StreamID:    fftypes.NewUUID(),
			BatchNumber: 1,
			Events: []*Event[testData]{
				{Data: &testData{Field1: 12345}},
			},
		})
		assert.Regexp(t, "FF00219", err)
		close(done)
	}()
	<-done
}

func TestWebhooksTLS(t *testing.T) {

	s := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(204)
	}))
	s.StartTLS()
	defer s.Close()

	u := s.URL
	tlsConfName := "tls0"
	wh := newTestWebhooks(t, &WebhookConfig{
		URL:           &u,
		TLSConfigName: &tlsConfName,
	}, func() {
		tls0 := TLSConfigs.ArrayEntry(0)
		tls0.Set(ConfigTLSConfigName, tlsConfName)
		tlsConf := tls0.SubSection("tls")
		// Would fail if this setting was not picked up
		tlsConf.Set(fftls.HTTPConfTLSInsecureSkipHostVerify, true)
	})

	done := make(chan struct{})
	go func() {
		err := wh.AttemptDispatch(context.Background(), 0, &EventBatch[testData]{
			StreamID:    fftypes.NewUUID(),
			BatchNumber: 1,
			Events: []*Event[testData]{
				{Data: &testData{Field1: 12345}},
			},
		})
		assert.NoError(t, err)
		close(done)
	}()
	<-done
}
