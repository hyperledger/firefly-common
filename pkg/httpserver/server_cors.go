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

package httpserver

import (
	"context"
	"net/http"

	"github.com/hyperledger/firefly-common/pkg/config"
	"github.com/hyperledger/firefly-common/pkg/log"
	"github.com/rs/cors"
)

func wrapCorsIfEnabled(ctx context.Context, prefix config.Prefix, chain http.Handler) http.Handler {
	if !prefix.GetBool(CorsEnabled) {
		return chain
	}
	corsOptions := cors.Options{
		AllowedOrigins:   prefix.GetStringSlice(CorsAllowedOrigins),
		AllowedMethods:   prefix.GetStringSlice(CorsAllowedMethods),
		AllowedHeaders:   prefix.GetStringSlice(CorsAllowedHeaders),
		AllowCredentials: prefix.GetBool(CorsAllowCredentials),
		MaxAge:           prefix.GetInt(CorsMaxAge),
		Debug:            prefix.GetBool(CorsDebug),
	}
	log.L(ctx).Debugf("CORS origins=%v methods=%v headers=%v creds=%t maxAge=%d",
		corsOptions.AllowedOrigins,
		corsOptions.AllowedMethods,
		corsOptions.AllowedHeaders,
		corsOptions.AllowCredentials,
		corsOptions.MaxAge,
	)
	c := cors.New(corsOptions)
	return c.Handler(chain)
}
