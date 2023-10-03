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

	"github.com/hyperledger/firefly-common/pkg/config"
	"github.com/hyperledger/firefly-common/pkg/ffresty"
	"github.com/hyperledger/firefly-common/pkg/fftls"
	"github.com/hyperledger/firefly-common/pkg/fftypes"
)

type Config struct {
	TLSConfigs        map[string]*fftls.Config `ffstruct:"EventStreamConfig" json:"tlsConfigs,omitempty"`
	WebSocketDefaults *ConfigWebsocketDefaults `ffstruct:"EventStreamConfig" json:"websockets,omitempty"`
	WebhookDefaults   *ConfigWebhookDefaults   `ffstruct:"EventStreamConfig" json:"webhooks,omitempty"`
}

type ConfigWebsocketDefaults struct {
	DefaultDistributionMode DistributionMode `ffstruct:"EventStreamConfig" json:"distributionMode"`
}

type ConfigWebhookDefaults struct {
	DisablePrivateIPs bool `ffstruct:"EventStreamConfig" json:"disabledPrivateIPs"`
	ffresty.HTTPConfig
}

const (
	ConfigTLSConfigName = "name"

	ConfigWebhooksDisablePrivateIPs = "disablePrivateIPs"
	ConfigWebhooksDefaultTLSConfig  = "tlsConfigName"

	ConfigWebSocketsDistributionMode = "distributionMode"
)

var RootConfig config.Section
var TLSConfigs config.ArraySection
var WebhookDefaultsConfig config.Section
var WebSocketsDefaultsConfig config.Section

// Due to how arrays work currently in the config system, this can only be initialized
// in one section for the whole process.
func InitConfig(conf config.Section) {

	RootConfig = conf
	TLSConfigs = RootConfig.SubArray("tlsConfigs")
	TLSConfigs.AddKnownKey(ConfigTLSConfigName)
	tlsSubSection := TLSConfigs.SubSection("tls")
	fftls.InitTLSConfig(tlsSubSection)
	tlsSubSection.SetDefault(fftls.HTTPConfTLSEnabled, true) // as it's a TLS config

	WebhookDefaultsConfig = conf.SubSection("webhooks")
	WebhookDefaultsConfig.AddKnownKey(ConfigWebhooksDisablePrivateIPs)
	ffresty.InitConfig(WebhookDefaultsConfig)

	WebSocketsDefaultsConfig = conf.SubSection("websockets")
	WebSocketsDefaultsConfig.AddKnownKey(ConfigWebSocketsDistributionMode, DistributionModeLoadBalance)
}

// Optional function to generate config directly from configuration.
// You can also generate the configuration programmatically
func GenerateConfig(ctx context.Context) *Config {
	httpDefaults, _ := ffresty.GenerateConfig(ctx, WebhookDefaultsConfig)
	tlsConfigs := map[string]*fftls.Config{}
	for i := 0; i < TLSConfigs.ArraySize(); i++ {
		tlsConf := TLSConfigs.ArrayEntry(i)
		name := tlsConf.GetString(ConfigTLSConfigName)
		tlsConfigs[name] = fftls.GenerateConfig(tlsConf.SubSection("tls"))
	}
	return &Config{
		TLSConfigs: tlsConfigs,
		WebSocketDefaults: &ConfigWebsocketDefaults{
			DefaultDistributionMode: fftypes.FFEnum(WebSocketsDefaultsConfig.GetString(ConfigWebSocketsDistributionMode)),
		},
		WebhookDefaults: &ConfigWebhookDefaults{
			DisablePrivateIPs: WebhookDefaultsConfig.GetBool(ConfigWebhooksDisablePrivateIPs),
			HTTPConfig:        httpDefaults.HTTPConfig,
		},
	}
}
