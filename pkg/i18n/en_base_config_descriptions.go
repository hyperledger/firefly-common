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

package i18n

import (
	"golang.org/x/text/language"
)

var TimeDurationType = "[`time.Duration`](https://pkg.go.dev/time#Duration)"
var TimeFormatType = "[Time format](https://pkg.go.dev/time#pkg-constants) `string`"
var ByteSizeType = "[`BytesSize`](https://pkg.go.dev/github.com/docker/go-units#BytesSize)"
var GoTemplateType = "[Go Template](https://pkg.go.dev/text/template) `string`"
var StringType = "`string`"
var ArrayStringType = "`[]string`"
var IntType = "`int`"
var BooleanType = "`boolean`"
var FloatType = "`float32`"
var MapStringStringType = "`map[string]string`"
var IgnoredType = "IGNORE"

var ffc = func(key, translation, fieldType string) ConfigMessageKey {
	return FFC(language.AmericanEnglish, key, translation, fieldType)
}

//revive:disable
var (
	ConfigGlobalConnectionTimeout = ffc("config.global.connectionTimeout", "The maximum amount of time that a connection is allowed to remain with no data transmitted", TimeDurationType)
	ConfigGlobalRequestTimeout    = ffc("config.global.requestTimeout", "The maximum amount of time that a request is allowed to remain open", TimeDurationType)

	ConfigGlobalRetryEnabled      = ffc("config.global.retry.enabled", "Enables retries", BooleanType)
	ConfigGlobalRetryFactor       = ffc("config.global.retry.factor", "The retry backoff factor", FloatType)
	ConfigGlobalRetryInitDelay    = ffc("config.global.retry.initDelay", "The initial retry delay", TimeDurationType)
	ConfigGlobalRetryInitialDelay = ffc("config.global.retry.initialDelay", "The initial retry delay", TimeDurationType)
	ConfigGlobalRetryMaxDelay     = ffc("config.global.retry.maxDelay", "The maximum retry delay", TimeDurationType)
	ConfigGlobalRetryMaxAttempts  = ffc("config.global.retry.maxAttempts", "The maximum number attempts", IntType)
	ConfigGlobalRetryCount        = ffc("config.global.retry.count", "The maximum number of times to retry", IntType)
	ConfigGlobalInitWaitTime      = ffc("config.global.retry.initWaitTime", "The initial retry delay", TimeDurationType)
	ConfigGlobalMaxWaitTime       = ffc("config.global.retry.maxWaitTime", "The maximum retry delay", TimeDurationType)

	ConfigGlobalUsername = ffc("config.global.auth.username", "Username", StringType)
	ConfigGlobalPassword = ffc("config.global.auth.password", "Password", StringType)
	ConfigGlobalProxyURL = ffc("config.global.proxy.url", "Optional HTTP proxy server to connect through", StringType)

	ConfigGlobalSize = ffc("config.global.cache.size", "The size of the cache", ByteSizeType)
	ConfigGlobalTTL  = ffc("config.global.cache.ttl", "The time to live (TTL) for the cache", TimeDurationType)

	ConfigGlobalWsHeartbeatInterval      = ffc("config.global.ws.heartbeatInterval", "The amount of time to wait between heartbeat signals on the WebSocket connection", TimeDurationType)
	ConfigGlobalWsInitialConnectAttempts = ffc("config.global.ws.initialConnectAttempts", "The number of attempts FireFly will make to connect to the WebSocket when starting up, before failing", IntType)
	ConfigGlobalWsPath                   = ffc("config.global.ws.path", "The WebSocket sever URL to which FireFly should connect", "WebSocket URL "+StringType)
	ConfigGlobalWsReadBufferSize         = ffc("config.global.ws.readBufferSize", "The size in bytes of the read buffer for the WebSocket connection", ByteSizeType)
	ConfigGlobalWsWriteBufferSize        = ffc("config.global.ws.writeBufferSize", "The size in bytes of the write buffer for the WebSocket connection", ByteSizeType)

	ConfigGlobalTLSCaFile           = ffc("config.global.tls.caFile", "The path to the CA file for TLS on this API", StringType)
	ConfigGlobalTLSCertFile         = ffc("config.global.tls.certFile", "The path to the certificate file for TLS on this API", StringType)
	ConfigGlobalTLSClientAuth       = ffc("config.global.tls.clientAuth", "Enables or disables client auth for TLS on this API", StringType)
	ConfigGlobalTLSEnabled          = ffc("config.global.tls.enabled", "Enables or disables TLS on this API", BooleanType)
	ConfigGlobalTLSKeyFile          = ffc("config.global.tls.keyFile", "The path to the private key file for TLS on this API", StringType)
	ConfigGlobalTLSHandshakeTimeout = ffc("config.global.tlsHandshakeTimeout", "The maximum amount of time to wait for a successful TLS handshake", TimeDurationType)

	ConfigGlobalBodyTemplate              = ffc("config.global.bodyTemplate", "The body go template string to use when making HTTP requests", GoTemplateType)
	ConfigGlobalCustomClient              = ffc("config.global.customClient", "Used for testing purposes only", IgnoredType)
	ConfigGlobalExpectContinueTimeout     = ffc("config.global.expectContinueTimeout", "See [ExpectContinueTimeout in the Go docs](https://pkg.go.dev/net/http#Transport)", TimeDurationType)
	ConfigGlobalHeaders                   = ffc("config.global.headers", "Adds custom headers to HTTP requests", MapStringStringType)
	ConfigGlobalIdleTimeout               = ffc("config.global.idleTimeout", "The max duration to hold a HTTP keepalive connection between calls", TimeDurationType)
	ConfigGlobalMaxIdleConns              = ffc("config.global.maxIdleConns", "The max number of idle connections to hold pooled", IntType)
	ConfigGlobalMethod                    = ffc("config.global.method", "The HTTP method to use when making requests to the Address Resolver", StringType)
	ConfigGlobalAuthType                  = ffc("config.global.auth.type", "The auth plugin to use for server side authentication of requests", StringType)
	ConfigGlobalPassthroughHeadersEnabled = ffc("config.global.passthroughHeadersEnabled", "Enable passing through the set of allowed HTTP request headers", BooleanType)

	ConfigLang                  = ffc("config.lang", "Default language for translation (API calls may support language override using headers)", StringType)
	ConfigLogCompress           = ffc("config.log.compress", "Determines if the rotated log files should be compressed using gzip", BooleanType)
	ConfigLogFilename           = ffc("config.log.filename", "Filename is the file to write logs to.  Backup log files will be retained in the same directory", StringType)
	ConfigLogFilesize           = ffc("config.log.filesize", "MaxSize is the maximum size the log file before it gets rotated", ByteSizeType)
	ConfigLogForceColor         = ffc("config.log.forceColor", "Force color to be enabled, even when a non-TTY output is detected", BooleanType)
	ConfigLogLevel              = ffc("config.log.level", "The log level - error, warn, info, debug, trace", StringType)
	ConfigLogMaxAge             = ffc("config.log.maxAge", "The maximum time to retain old log files based on the timestamp encoded in their filename.", TimeDurationType)
	ConfigLogMaxBackups         = ffc("config.log.maxBackups", "Maximum number of old log files to retain", IntType)
	ConfigLogNoColor            = ffc("config.log.noColor", "Force color to be disabled, event when TTY output is detected", BooleanType)
	ConfigLogTimeFormat         = ffc("config.log.timeFormat", "Custom time format for logs", TimeFormatType)
	ConfigLogUtc                = ffc("config.log.utc", "Use UTC timestamps for logs", BooleanType)
	ConfigLogIncludeCodeInfo    = ffc("config.log.includeCodeInfo", "Enables the report caller for including the calling file and line number, and the calling function. If using text logs, it uses the logrus text format rather than the default prefix format.", BooleanType)
	ConfigLogJSONEnabled        = ffc("config.log.json.enabled", "Enables JSON formatted logs rather than text. All log color settings are ignored when enabled.", BooleanType)
	ConfigLogJSONTimestampField = ffc("config.log.json.fields.timestamp", "Configures the JSON key containing the timestamp of the log", StringType)
	ConfigLogJSONLevelField     = ffc("config.log.json.fields.level", "Configures the JSON key containing the log level", StringType)
	ConfigLogJSONMessageField   = ffc("config.log.json.fields.message", "Configures the JSON key containing the log message", StringType)
	ConfigLogJSONFuncField      = ffc("config.log.json.fields.func", "Configures the JSON key containing the calling function", StringType)
	ConfigLogJSONFileField      = ffc("config.log.json.fields.file", "configures the JSON key containing the calling file", StringType)
	ConfigCorsCredentials       = ffc("config.cors.credentials", "CORS setting to control whether a browser allows credentials to be sent to this API", BooleanType)

	ConfigCorsDebug   = ffc("config.global.cors.debug", "Whether debug is enabled for the CORS implementation", BooleanType)
	ConfigCorsEnabled = ffc("config.global.cors.enabled", "Whether CORS is enabled", BooleanType)
	ConfigCorsHeaders = ffc("config.global.cors.headers", "CORS setting to control the allowed headers", ArrayStringType)
	ConfigCorsMaxAge  = ffc("config.global.cors.maxAge", "The maximum age a browser should rely on CORS checks", TimeDurationType)
	ConfigCorsMethods = ffc("config.global.cors.methods", " CORS setting to control the allowed methods", ArrayStringType)
	ConfigCorsOrigins = ffc("config.global.cors.origins", "CORS setting to control the allowed origins", ArrayStringType)

	ConfigGlobalAuthBasicPasswordFile = ffc("config.global.basic.passwordfile", "The path to a .htpasswd file to use for authenticating requests. Passwords should be hashed with bcrypt.", StringType)

	ConfigGlobalDebugEnabled = ffc("config.debug.enabled", "Whether the debug HTTP endpoint is enabled", BooleanType)

	ConfigGlobalPort            = ffc("config.global.port", "Listener port", IntType)
	ConfigGlobalAddress         = ffc("config.global.address", "Listener address", IntType)
	ConfigGlobalPublicURL       = ffc("config.global.publicURL", "Externally available URL for the HTTP endpoint", StringType)
	ConfigGlobalReadTimeout     = ffc("config.global.readTimeout", "HTTP server read timeout", TimeDurationType)
	ConfigGlobalWriteTimeout    = ffc("config.global.writeTimeout", "HTTP server write timeout", TimeDurationType)
	ConfigGlobalShutdownTimeout = ffc("config.global.shutdownTimeout", "HTTP server shutdown timeout", TimeDurationType)
)
