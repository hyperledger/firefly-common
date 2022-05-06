package wsclient

import (
	"testing"
	"time"

	"github.com/hyperledger/firefly-common/pkg/config"
	"github.com/hyperledger/firefly-common/pkg/ffresty"
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

	wsConfig := GenerateConfig(utConf)

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
}
