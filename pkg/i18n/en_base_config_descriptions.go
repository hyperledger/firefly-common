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

package i18n

var TimeDurationType = "[`time.Duration`](https://pkg.go.dev/time#Duration)"
var TimeFormatType = "[Time format](https://pkg.go.dev/time#pkg-constants) `string`"
var ByteSizeType = "[`BytesSize`](https://pkg.go.dev/github.com/docker/go-units#BytesSize)"
var GoTemplateType = "[Go Template](https://pkg.go.dev/text/template) `string`"
var StringType = "`string`"
var IntType = "`int`"
var BooleanType = "`boolean`"
var FloatType = "`boolean`"
var MapStringStringType = "`map[string]string`"
var IgnoredType = "IGNORE"

//revive:disable
var (
	ConfigGlobalConnectionTimeout = FFC("config.global.connectionTimeout", "The maximum amount of time that a connection is allowed to remain with no data transmitted", TimeDurationType)
	ConfigGlobalRequestTimeout    = FFC("config.global.requestTimeout", "The maximum amount of time that a request is allowed to remain open", TimeDurationType)

	ConfigGlobalRetryEnabled      = FFC("config.global.retry.enabled", "Enables retries", BooleanType)
	ConfigGlobalRetryFactor       = FFC("config.global.retry.factor", "The retry backoff factor", FloatType)
	ConfigGlobalRetryInitDelay    = FFC("config.global.retry.initDelay", "The initial retry delay", TimeDurationType)
	ConfigGlobalRetryInitialDelay = FFC("config.global.retry.initialDelay", "The initial retry delay", TimeDurationType)
	ConfigGlobalRetryMaxDelay     = FFC("config.global.retry.maxDelay", "The maximum retry delay", TimeDurationType)
	ConfigGlobalRetryMaxAttempts  = FFC("config.global.retry.maxAttempts", "The maximum number attempts", IntType)
	ConfigGlobalRetryCount        = FFC("config.global.retry.count", "The maximum number of times to retry", IntType)
	ConfigGlobalInitWaitTime      = FFC("config.global.retry.initWaitTime", "The initial retry delay", TimeDurationType)
	ConfigGlobalMaxWaitTime       = FFC("config.global.retry.maxWaitTime", "The maximum retry delay", TimeDurationType)

	ConfigGlobalUsername = FFC("config.global.auth.username", "Username", StringType)
	ConfigGlobalPassword = FFC("config.global.auth.password", "Password", StringType)

	ConfigGlobalSize = FFC("config.global.cache.size", "The size of the cache", ByteSizeType)
	ConfigGlobalTTL  = FFC("config.global.cache.ttl", "The time to live (TTL) for the cache", TimeDurationType)

	ConfigGlobaltWsHeartbeatInterval     = FFC("config.global.ws.heartbeatInterval", "The amount of time to wait between heartbeat signals on the WebSocket connection", TimeDurationType)
	ConfigGlobalWsInitialConnectAttempts = FFC("config.global.ws.initialConnectAttempts", "The number of attempts FireFly will make to connect to the WebSocket when starting up, before failing", IntType)
	ConfigGlobalWsPath                   = FFC("config.global.ws.path", "The WebSocket sever URL to which FireFly should connect", "WebSocket URL "+StringType)
	ConfigGlobalWsReadBufferSize         = FFC("config.global.ws.readBufferSize", "The size in bytes of the read buffer for the WebSocket connection", ByteSizeType)
	ConfigGlobalWsWriteBufferSize        = FFC("config.global.ws.writeBufferSize", "The size in bytes of the write buffer for the WebSocket connection", ByteSizeType)

	ConfigGlobalTLSCaFile           = FFC("config.global.tls.caFile", "The path to the CA file for TLS on this API", StringType)
	ConfigGlobalTLSCertFile         = FFC("config.global.tls.certFile", "The path to the certificate file for TLS on this API", StringType)
	ConfigGlobalTLSClientAuth       = FFC("config.global.tls.clientAuth", "Enables or disables client auth for TLS on this API", StringType)
	ConfigGlobalTLSEnabled          = FFC("config.global.tls.enabled", "Enables or disables TLS on this API", BooleanType)
	ConfigGlobalTLSKeyFile          = FFC("config.global.tls.keyFile", "The path to the private key file for TLS on this API", StringType)
	ConfigGlobalTLSHandshakeTimeout = FFC("config.global.tlsHandshakeTimeout", "The maximum amount of time to wait for a successful TLS handshake", TimeDurationType)

	ConfigGlobalBodyTemplate          = FFC("config.global.bodyTemplate", "The body go template string to use when making HTTP requests", GoTemplateType)
	ConfigGlobalCustomClient          = FFC("config.global.customClient", "Used for testing purposes only", IgnoredType)
	ConfigGlobalExpectContinueTimeout = FFC("config.global.expectContinueTimeout", "See [ExpectContinueTimeout in the Go docs](https://pkg.go.dev/net/http#Transport)", TimeDurationType)
	ConfigGlobalHeaders               = FFC("config.global.headers", "Adds custom headers to HTTP requests", MapStringStringType)
	ConfigGlobalIdleTimeout           = FFC("config.global.idleTimeout", "The max duration to hold a HTTP keepalive connection between calls", TimeDurationType)
	ConfigGlobalMaxIdleConns          = FFC("config.global.maxIdleConns", "The max number of idle connections to hold pooled", IntType)
	ConfigGlobalMethod                = FFC("config.global.method", "The HTTP method to use when making requests to the Address Resolver", StringType)

	ConfigLang                  = FFC("config.lang", "Default language for translation (API calls may support language override using headers)", StringType)
	ConfigLogCompress           = FFC("config.log.compress", "Determines if the rotated log files should be compressed using gzip", BooleanType)
	ConfigLogFilename           = FFC("config.log.filename", "Filename is the file to write logs to.  Backup log files will be retained in the same directory", StringType)
	ConfigLogFilesize           = FFC("config.log.filesize", "MaxSize is the maximum size the log file before it gets rotated", ByteSizeType)
	ConfigLogForceColor         = FFC("config.log.forceColor", "Force color to be enabled, even when a non-TTY output is detected", BooleanType)
	ConfigLogLevel              = FFC("config.log.level", "The log level - error, warn, info, debug, trace", StringType)
	ConfigLogMaxAge             = FFC("config.log.maxAge", "The maximum time to retain old log files based on the timestamp encoded in their filename.", TimeDurationType)
	ConfigLogMaxBackups         = FFC("config.log.maxBackups", "Maximum number of old log files to retain", IntType)
	ConfigLogNoColor            = FFC("config.log.noColor", "Force color to be disabled, event when TTY output is detected", BooleanType)
	ConfigLogTimeFormat         = FFC("config.log.timeFormat", "Custom time format for logs", TimeFormatType)
	ConfigLogUtc                = FFC("config.log.utc", "Use UTC timestamps for logs", BooleanType)
	ConfigLogIncludeCodeInfo    = FFC("config.log.includeCodeInfo", "Enables the report caller for including the calling file and line number, and the calling function. If using text logs, it uses the logrus text format rather than the default prefix format.", BooleanType)
	ConfigLogJSONEnabled        = FFC("config.log.json.enabled", "Enables JSON formatted logs rather than text. All log color settings are ignored when enabled.", BooleanType)
	ConfigLogJSONTimestampField = FFC("config.log.json.fields.timestamp", "Configures the JSON key containing the timestamp of the log", StringType)
	ConfigLogJSONLevelField     = FFC("config.log.json.fields.level", "Configures the JSON key containing the log level", StringType)
	ConfigLogJSONMessageField   = FFC("config.log.json.fields.message", "Configures the JSON key containing the log message", StringType)
	ConfigLogJSONFuncField      = FFC("config.log.json.fields.func", "Configures the JSON key containing the calling function", StringType)
	ConfigLogJSONFileField      = FFC("config.log.json.fields.file", "configures the JSON key containing the calling file", StringType)
	ConfigCorsCredentials       = FFC("config.cors.credentials", "CORS setting to control whether a browser allows credentials to be sent to this API", BooleanType)

	ConfigCorsDebug   = FFC("config.global.cors.debug", "Whether debug is enabled for the CORS implementation", BooleanType)
	ConfigCorsEnabled = FFC("config.global.cors.enabled", "Whether CORS is enabled", BooleanType)
	ConfigCorsHeaders = FFC("config.global.cors.headers", "CORS setting to control the allowed headers", StringType)
	ConfigCorsMaxAge  = FFC("config.global.cors.maxAge", "The maximum age a browser should rely on CORS checks", TimeDurationType)
	ConfigCorsMethods = FFC("config.global.cors.methods", " CORS setting to control the allowed methods", StringType)
	ConfigCorsOrigins = FFC("config.global.cors.origins", "CORS setting to control the allowed origins", StringType)
)
