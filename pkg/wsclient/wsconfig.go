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

package wsclient

import (
	"context"
	"time"

	"github.com/hyperledger/firefly-common/pkg/config"
	"github.com/hyperledger/firefly-common/pkg/ffresty"
	"github.com/hyperledger/firefly-common/pkg/fftls"
)

const (
	defaultInitialConnectAttempts = 5
	defaultBufferSize             = "16Kb"
	defaultHeartbeatInterval      = "30s"            // up to a minute to detect a dead connection
	defaultConnectionTimeout      = 45 * time.Second // 45 seconds - the built in default for gorilla/websocket
)

const (
	// WSSpecificConfPrefix is the named sub-section of the http config options that contains websocket specific config
	WSSpecificConfPrefix = "ws"
	// WSConfigKeyWriteBufferSize is the write buffer size
	WSConfigKeyWriteBufferSize = "ws.writeBufferSize"
	// WSConfigKeyReadBufferSize is the read buffer size
	WSConfigKeyReadBufferSize = "ws.readBufferSize"
	// WSConfigKeyInitialConnectAttempts sets how many times the websocket should attempt to connect on startup, before failing (after initial connection, retry is indefinite)
	WSConfigKeyInitialConnectAttempts = "ws.initialConnectAttempts"
	// WSConfigKeyPath if set will define the path to connect to - allows sharing of the same URL between HTTP and WebSocket connection info
	WSConfigKeyPath = "ws.path"
	// WSConfigKeyHeartbeatInterval is the frequency of ping/pong requests, and also used for the timeout to receive a response to the heartbeat
	WSConfigKeyHeartbeatInterval = "ws.heartbeatInterval"
	// WSConnectionTimeout is the amount of time to wait while attempting to establish a connection (or automatic reconnection)
	WSConfigKeyConnectionTimeout = "ws.connectionTimeout"
)

// InitConfig ensures the config is initialized for HTTP too, as WS and HTTP
// can share the same tree of configuration (and all the HTTP options apply to the initial upgrade)
func InitConfig(conf config.Section) {
	ffresty.InitConfig(conf)
	conf.AddKnownKey(WSConfigKeyWriteBufferSize, defaultBufferSize)
	conf.AddKnownKey(WSConfigKeyReadBufferSize, defaultBufferSize)
	conf.AddKnownKey(WSConfigKeyInitialConnectAttempts, defaultInitialConnectAttempts)
	conf.AddKnownKey(WSConfigKeyPath)
	conf.AddKnownKey(WSConfigKeyHeartbeatInterval, defaultHeartbeatInterval)
	conf.AddKnownKey(WSConfigKeyConnectionTimeout, defaultConnectionTimeout)
}

func GenerateConfig(ctx context.Context, conf config.Section) (*WSConfig, error) {
	wsConfig := &WSConfig{
		HTTPURL:                conf.GetString(ffresty.HTTPConfigURL),
		WSKeyPath:              conf.GetString(WSConfigKeyPath),
		ReadBufferSize:         int(conf.GetByteSize(WSConfigKeyReadBufferSize)),
		WriteBufferSize:        int(conf.GetByteSize(WSConfigKeyWriteBufferSize)),
		InitialDelay:           conf.GetDuration(ffresty.HTTPConfigRetryInitDelay),
		MaximumDelay:           conf.GetDuration(ffresty.HTTPConfigRetryMaxDelay),
		InitialConnectAttempts: conf.GetInt(WSConfigKeyInitialConnectAttempts),
		HTTPHeaders:            conf.GetObject(ffresty.HTTPConfigHeaders),
		AuthUsername:           conf.GetString(ffresty.HTTPConfigAuthUsername),
		AuthPassword:           conf.GetString(ffresty.HTTPConfigAuthPassword),
		HeartbeatInterval:      conf.GetDuration(WSConfigKeyHeartbeatInterval),
		ConnectionTimeout:      conf.GetDuration(WSConfigKeyConnectionTimeout),
	}
	tlsSection := conf.SubSection("tls")
	tlsClientConfig, err := fftls.ConstructTLSConfig(ctx, tlsSection, fftls.ClientType)
	if err != nil {
		return nil, err
	}

	wsConfig.TLSClientConfig = tlsClientConfig

	return wsConfig, nil
}
