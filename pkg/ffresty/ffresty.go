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

package ffresty

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/go-resty/resty/v2"
	"github.com/hyperledger/firefly-common/pkg/config"
	"github.com/hyperledger/firefly-common/pkg/ffapi"
	"github.com/hyperledger/firefly-common/pkg/fftypes"
	"github.com/hyperledger/firefly-common/pkg/i18n"
	"github.com/hyperledger/firefly-common/pkg/log"
	"github.com/sirupsen/logrus"
)

type retryCtxKey struct{}

type retryCtx struct {
	id       string
	start    time.Time
	attempts uint
}

type Config struct {
	URL                           string             `json:"httpURL,omitempty"`
	ProxyURL                      string             `json:"proxyURL,omitempty"`
	HTTPRequestTimeout            time.Duration      `json:"requestTimeout,omitempty"`
	HTTPIdleConnTimeout           time.Duration      `json:"idleTimeout,omitempty"`
	HTTPMaxIdleTimeout            time.Duration      `json:"maxIdleTimeout,omitempty"`
	HTTPConnectionTimeout         time.Duration      `json:"connectionTimeout,omitempty"`
	HTTPExpectContinueTimeout     time.Duration      `json:"expectContinueTimeout,omitempty"`
	AuthUsername                  string             `json:"authUsername,omitempty"`
	AuthPassword                  string             `json:"authPassword,omitempty"`
	Retry                         bool               `json:"retry,omitempty"`
	RetryCount                    int                `json:"retryCount,omitempty"`
	RetryInitialDelay             time.Duration      `json:"retryInitialDelay,omitempty"`
	RetryMaximumDelay             time.Duration      `json:"retryMaximumDelay,omitempty"`
	HTTPMaxIdleConns              int                `json:"maxIdleConns,omitempty"`
	HTTPPassthroughHeadersEnabled bool               `json:"httpPassthroughHeadersEnabled,omitempty"`
	HTTPHeaders                   fftypes.JSONObject `json:"headers,omitempty"`
	TLSClientConfig               *tls.Config        `json:"tlsClientConfig,omitempty"`
	HTTPTLSHandshakeTimeout       time.Duration      `json:"tlsHandshakeTimeout,omitempty"`
	HTTPCustomClient              interface{}        `json:"httpCustomClient,omitempty"`
}

// OnAfterResponse when using SetDoNotParseResponse(true) for streaming binary replies,
// the caller should invoke ffresty.OnAfterResponse on the response manually.
// The middleware is disabled on this path :-(
// See: https://github.com/go-resty/resty/blob/d01e8d1bac5ba1fed0d9e03c4c47ca21e94a7e8e/client.go#L912-L948
func OnAfterResponse(c *resty.Client, resp *resty.Response) {
	if c == nil || resp == nil {
		return
	}
	rCtx := resp.Request.Context()
	rc := rCtx.Value(retryCtxKey{}).(*retryCtx)
	elapsed := float64(time.Since(rc.start)) / float64(time.Millisecond)
	level := logrus.DebugLevel
	status := resp.StatusCode()
	if status >= 300 {
		level = logrus.ErrorLevel
	}
	log.L(rCtx).Logf(level, "<== %s %s [%d] (%.2fms)", resp.Request.Method, resp.Request.URL, status, elapsed)
}

// New creates a new Resty client, using static configuration (from the config file)
// from a given section in the static configuration
//
// You can use the normal Resty builder pattern, to set per-instance configuration
// as required.
func New(ctx context.Context, staticConfig config.Section) (client *resty.Client, err error) {
	ffrestyConfig, err := GenerateConfig(ctx, staticConfig)
	if err != nil {
		return nil, err
	}

	return NewWithConfig(ctx, ffrestyConfig)
}

// New creates a new Resty client, using static configuration (from the config file)
// from a given section in the static configuration
//
// You can use the normal Resty builder pattern, to set per-instance configuration
// as required.
func NewWithConfig(ctx context.Context, ffrestyConfig *Config) (client *resty.Client, err error) {
	// passthroughHeadersEnabled := staticConfig.GetBool(HTTPPassthroughHeadersEnabled)

	if ffrestyConfig.HTTPCustomClient != nil {
		if httpClient, ok := ffrestyConfig.HTTPCustomClient.(*http.Client); ok {
			client = resty.NewWithClient(httpClient)
		}
	}
	if client == nil {

		httpTransport := &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   ffrestyConfig.HTTPConnectionTimeout,
				KeepAlive: ffrestyConfig.HTTPConnectionTimeout,
			}).DialContext,
			ForceAttemptHTTP2:     true,
			MaxIdleConns:          ffrestyConfig.HTTPMaxIdleConns,
			IdleConnTimeout:       ffrestyConfig.HTTPIdleConnTimeout,
			TLSHandshakeTimeout:   ffrestyConfig.HTTPTLSHandshakeTimeout,
			ExpectContinueTimeout: ffrestyConfig.HTTPExpectContinueTimeout,
		}

		if ffrestyConfig.TLSClientConfig != nil {
			httpTransport.TLSClientConfig = ffrestyConfig.TLSClientConfig
		}

		httpClient := &http.Client{
			Transport: httpTransport,
		}
		client = resty.NewWithClient(httpClient)
	}

	url := strings.TrimSuffix(ffrestyConfig.URL, "/")
	if url != "" {
		client.SetBaseURL(url)
		log.L(ctx).Debugf("Created REST client to %s", url)
	}

	if ffrestyConfig.ProxyURL != "" {
		client.SetProxy(ffrestyConfig.ProxyURL)
	}

	client.SetTimeout(ffrestyConfig.HTTPRequestTimeout)

	client.OnBeforeRequest(func(c *resty.Client, req *resty.Request) error {
		rCtx := req.Context()
		rc := rCtx.Value(retryCtxKey{})
		if rc == nil {
			// First attempt
			r := &retryCtx{
				id:    fftypes.ShortID(),
				start: time.Now(),
			}
			rCtx = context.WithValue(rCtx, retryCtxKey{}, r)
			// Create a request logger from the root logger passed into the client
			l := log.L(ctx).WithField("breq", r.id)
			rCtx = log.WithLogger(rCtx, l)
			req.SetContext(rCtx)
		}

		// If passthroughHeaders: true for this rest client, pass any of the allowed headers on the original req
		if ffrestyConfig.HTTPPassthroughHeadersEnabled {
			ctxHeaders := rCtx.Value(ffapi.CtxHeadersKey{})
			if ctxHeaders != nil {
				passthroughHeaders := ctxHeaders.(http.Header)
				for key := range passthroughHeaders {
					req.Header.Set(key, passthroughHeaders.Get(key))
				}
			}
		}

		// If an X-FireFlyRequestID was set on the context, pass that header on this request too
		ffRequestID := rCtx.Value(ffapi.CtxFFRequestIDKey{})
		if ffRequestID != nil {
			req.Header.Set(ffapi.FFRequestIDHeader, ffRequestID.(string))
		}

		log.L(rCtx).Debugf("==> %s %s%s", req.Method, url, req.URL)
		return nil
	})

	// Note that callers using SetNotParseResponse will need to invoke this themselves

	client.OnAfterResponse(func(c *resty.Client, r *resty.Response) error { OnAfterResponse(c, r); return nil })

	for k, v := range ffrestyConfig.HTTPHeaders {
		if vs, ok := v.(string); ok {
			client.SetHeader(k, vs)
		}
	}
	if ffrestyConfig.AuthUsername != "" && ffrestyConfig.AuthPassword != "" {
		client.SetHeader("Authorization", fmt.Sprintf("Basic %s", base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%s:%s", ffrestyConfig.AuthUsername, ffrestyConfig.AuthPassword)))))
	}

	if ffrestyConfig.Retry {
		retryCount := ffrestyConfig.RetryCount
		minTimeout := ffrestyConfig.RetryInitialDelay
		maxTimeout := ffrestyConfig.RetryMaximumDelay
		client.
			SetRetryCount(retryCount).
			SetRetryWaitTime(minTimeout).
			SetRetryMaxWaitTime(maxTimeout).
			AddRetryCondition(func(r *resty.Response, err error) bool {
				if r == nil || r.IsSuccess() {
					return false
				}
				rCtx := r.Request.Context()
				rc := rCtx.Value(retryCtxKey{}).(*retryCtx)
				log.L(rCtx).Infof("retry %d/%d (min=%dms/max=%dms) status=%d", rc.attempts, retryCount, minTimeout.Milliseconds(), maxTimeout.Milliseconds(), r.StatusCode())
				rc.attempts++
				return true
			})
	}

	return client, nil
}

func WrapRestErr(ctx context.Context, res *resty.Response, err error, key i18n.ErrorMessageKey) error {
	var respData string
	if res != nil {
		if res.RawBody() != nil {
			defer func() { _ = res.RawBody().Close() }()
			if r, err := io.ReadAll(res.RawBody()); err == nil {
				respData = string(r)
			}
		}
		if respData == "" {
			respData = res.String()
		}
		if len(respData) > 256 {
			respData = respData[0:256] + "..."
		}
	}
	if err != nil {
		return i18n.WrapError(ctx, err, key, respData)
	}
	return i18n.NewError(ctx, key, respData)
}
