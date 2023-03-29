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

package httpserver

import (
	"context"
	"fmt"
	"net/http"
	"net/http/pprof"
	"time"

	"github.com/gorilla/mux"
	"github.com/hyperledger/firefly-common/pkg/config"
	"github.com/hyperledger/firefly-common/pkg/log"
)

func RunDebugServer(ctx context.Context, debugServerConf config.Section) {
	var debugServer *http.Server
	debugPort := debugServerConf.GetInt(HTTPConfPort)
	debugAddress := debugServerConf.GetString(HTTPConfAddress)
	debugEnabled := debugServerConf.GetBool(DebugEnabled)
	if debugEnabled {
		r := mux.NewRouter()
		r.PathPrefix("/debug/pprof/cmdline").HandlerFunc(pprof.Cmdline)
		r.PathPrefix("/debug/pprof/profile").HandlerFunc(pprof.Profile)
		r.PathPrefix("/debug/pprof/symbol").HandlerFunc(pprof.Symbol)
		r.PathPrefix("/debug/pprof/trace").HandlerFunc(pprof.Trace)
		r.PathPrefix("/debug/pprof/").HandlerFunc(pprof.Index)
		debugServer = &http.Server{Addr: fmt.Sprintf("%s:%d", debugAddress, debugPort), Handler: r, ReadHeaderTimeout: 30 * time.Second}
		go func() {
			_ = debugServer.ListenAndServe()
		}()
		log.L(ctx).Debugf("Debug HTTP endpoint listening on %s:%d", debugAddress, debugPort)
	}

	<-ctx.Done()
	if debugServer != nil {
		_ = debugServer.Close()
	}
}
