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
	"crypto/tls"
	"database/sql/driver"
	"net"
	"net/url"

	"github.com/go-resty/resty/v2"
	"github.com/hyperledger/firefly-common/pkg/ffresty"
	"github.com/hyperledger/firefly-common/pkg/fftypes"
	"github.com/hyperledger/firefly-common/pkg/i18n"
	"github.com/hyperledger/firefly-common/pkg/log"
)

type WebhookConfig struct {
	URL           *string             `ffstruct:"whconfig" json:"url,omitempty"`
	Headers       map[string]string   `ffstruct:"whconfig" json:"headers,omitempty"`
	TLSConfigName *string             `ffstruct:"whconfig" json:"tlsConfigName,omitempty"`
	HTTP          *ffresty.HTTPConfig `ffstruct:"whconfig" json:"http,omitempty"`
	validated     bool
	tlsConfig     *tls.Config
}

// Store in DB as JSON
func (wc *WebhookConfig) Scan(src interface{}) error {
	return fftypes.JSONScan(src, wc)
}

// Store in DB as JSON
func (wc *WebhookConfig) Value() (driver.Value, error) {
	return fftypes.JSONValue(wc)
}

// Validate initializes the config ready for use
func (wc *WebhookConfig) Validate(ctx context.Context, es *esManager) error {
	if wc.URL == nil || *wc.URL == "" {
		return i18n.NewError(ctx, i18n.MsgMissingWebhookURL)
	}
	if wc.TLSConfigName != nil && *wc.TLSConfigName != "" {
		tlsConfig, ok := es.tlsConfigs[*wc.TLSConfigName]
		if !ok {
			return i18n.NewError(ctx, i18n.MsgUnknownTLSConfiguration, *wc.TLSConfigName)
		}
		wc.tlsConfig = tlsConfig
	}
	wc.validated = true
	return nil
}

type webhookAction struct {
	es                *esManager
	disablePrivateIPs bool
	spec              *WebhookConfig
	client            *resty.Client
}

func (es *esManager) newWebhookAction(ctx context.Context, spec *WebhookConfig) (*webhookAction, error) {
	if !spec.validated {
		return nil, i18n.NewError(ctx, i18n.MsgConfigurationNotValidated)
	}
	conf := spec.HTTP
	if conf == nil {
		conf = &es.config.WebhookDefaults.HTTPConfig
	}
	conf.TLSClientConfig = spec.tlsConfig
	client := ffresty.NewWithConfig(ctx, ffresty.Config{
		URL:        *spec.URL,
		HTTPConfig: *conf,
	})
	return &webhookAction{
		es:                es,
		spec:              spec,
		disablePrivateIPs: es.config.WebhookDefaults.DisablePrivateIPs,
		client:            client,
	}, nil
}

// attemptWebhookAction performs a single attempt of a webhook action
func (w *webhookAction) attemptDispatch(ctx context.Context, batchNumber int64, attempt int, body interface{}) error {
	// We perform DNS resolution before each attempt, to exclude private IP address ranges from the target
	u, _ := url.Parse(*w.spec.URL)
	addr, err := net.ResolveIPAddr("ip4", u.Hostname())
	if err != nil {
		return i18n.NewError(ctx, i18n.MsgInvalidHost, u.Hostname())
	}
	if w.isAddressBlocked(addr) {
		return i18n.NewError(ctx, i18n.MsgBlockWebhookAddress, addr, u.Hostname())
	}
	var resBody []byte
	req := w.client.R().
		SetContext(ctx).
		SetBody(body).
		SetResult(&resBody).
		SetError(&resBody)
	req.Header.Set("Content-Type", "application/json")
	for h, v := range w.spec.Headers {
		req.Header.Set(h, v)
	}
	res, err := req.Post(u.String())
	if err != nil {
		log.L(ctx).Errorf("Webhook %s (%s) batch=%d attempt=%d: %s", *w.spec.URL, u, batchNumber, attempt, err)
		return i18n.NewError(ctx, i18n.MsgWebhookErr, err)
	}
	if res.IsError() {
		log.L(ctx).Errorf("Webhook %s (%s) [%d] batch=%d attempt=%d: %s", *w.spec.URL, u, res.StatusCode(), batchNumber, attempt, resBody)
		err = i18n.NewError(ctx, i18n.MsgWebhookFailedStatus, res.StatusCode())
	}
	return err
}

// isAddressBlocked allows blocking of all of the "private" address blocks defined by IPv4
func (w *webhookAction) isAddressBlocked(ip *net.IPAddr) bool {
	ip4 := ip.IP.To4()
	return w.disablePrivateIPs &&
		(ip4[0] == 0 ||
			ip4[0] >= 224 ||
			ip4[0] == 127 ||
			ip4[0] == 10 ||
			(ip4[0] == 172 && ip4[1] >= 16 && ip4[1] < 32) ||
			(ip4[0] == 192 && ip4[1] == 168))
}
