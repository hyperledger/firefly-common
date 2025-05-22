package wsclient

import (
	"context"
	"testing"
	"time"

	"github.com/hyperledger/firefly-common/pkg/config"
	"github.com/hyperledger/firefly-common/pkg/ffresty"
	"github.com/hyperledger/firefly-common/pkg/fftls"
	"github.com/stretchr/testify/assert"
)

var utConf = config.RootSection("ws")

func resetConf() {
	config.RootConfigReset()
	InitConfig(utConf)
}

func TestWSConfigGeneration(t *testing.T) {
	resetConf()

	utConf.Set(ffresty.HTTPConfigURL, "http://test:12345")
	utConf.Set(ffresty.HTTPConfigHeaders, map[string]interface{}{
		"custom-header": "custom value",
	})
	utConf.Set(ffresty.HTTPConfigAuthUsername, "user")
	utConf.Set(ffresty.HTTPConfigAuthPassword, "pass")
	utConf.Set(ffresty.HTTPConfigRetryInitDelay, 1)
	utConf.Set(ffresty.HTTPConfigRetryMaxDelay, 1)
	utConf.Set(WSConfigKeyReadBufferSize, 1024)
	utConf.Set(WSConfigKeyWriteBufferSize, 1024)
	utConf.Set(WSConfigKeyInitialConnectAttempts, 1)
	utConf.Set(WSConfigKeyPath, "/websocket")
	utConf.Set(WSConfigKeyBackgroundConnect, true)
	utConf.Set(WSConfigKeyHeartbeatInterval, "42ms")

	ctx := context.Background()
	wsConfig, err := GenerateConfig(ctx, utConf)
	assert.NoError(t, err)

	assert.Equal(t, "http://test:12345", wsConfig.HTTPURL)
	assert.Equal(t, "user", wsConfig.AuthUsername)
	assert.Equal(t, "pass", wsConfig.AuthPassword)
	assert.Equal(t, time.Duration(1000000), wsConfig.InitialDelay)
	assert.Equal(t, time.Duration(1000000), wsConfig.MaximumDelay)
	assert.Equal(t, 1, wsConfig.InitialConnectAttempts)
	assert.Equal(t, "/websocket", wsConfig.WSKeyPath)
	assert.Equal(t, "custom value", wsConfig.HTTPHeaders.GetString("custom-header"))
	assert.Equal(t, 1024, wsConfig.ReadBufferSize)
	assert.Equal(t, 1024, wsConfig.WriteBufferSize)
	assert.True(t, wsConfig.BackgroundConnect)
	assert.Equal(t, 42*time.Millisecond, wsConfig.HeartbeatInterval)
}

func TestWSConfigGenerationDefaults(t *testing.T) {
	resetConf()

	ctx := context.Background()
	wsConfig, err := GenerateConfig(ctx, utConf)
	assert.NoError(t, err)

	assert.Equal(t, defaultInitialConnectAttempts, wsConfig.InitialConnectAttempts)
	assert.False(t, wsConfig.BackgroundConnect)
	assert.Equal(t, 30*time.Second, wsConfig.HeartbeatInterval)
}

func TestWSConfigTLSGenerationFail(t *testing.T) {
	resetConf()

	tlsSection := utConf.SubSection("tls")
	tlsSection.Set(fftls.HTTPConfTLSEnabled, true)
	tlsSection.Set(fftls.HTTPConfTLSCAFile, "bad-ca")

	ctx := context.Background()
	_, err := GenerateConfig(ctx, utConf)
	assert.Regexp(t, "FF00153", err)
}
