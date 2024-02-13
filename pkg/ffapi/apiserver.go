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

package ffapi

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/gorilla/mux"
	"github.com/hyperledger/firefly-common/pkg/config"
	"github.com/hyperledger/firefly-common/pkg/fftls"
	"github.com/hyperledger/firefly-common/pkg/httpserver"
	"github.com/hyperledger/firefly-common/pkg/i18n"
	"github.com/hyperledger/firefly-common/pkg/metric"
)

const APIServerMetricsSubSystemName = "api_server_rest"

// APIServer is an opinionated use of the HTTP Server facilities in common, to provide
// an API server with:
// - a set of routes defining API endpoints
// - a swagger endpoint on /api
// - optional metrics endpoint on /metrics (configurable)
type APIServer interface {
	Serve(ctx context.Context) error
	Started() <-chan struct{}
	MuxRouter(ctx context.Context) *mux.Router
	APIPublicURL() string // valid to call after server is successfully started
}

type apiServer[T any] struct {
	started chan struct{}

	defaultFilterLimit        uint64
	maxFilterLimit            uint64
	maxFilterSkip             uint64
	requestTimeout            time.Duration
	requestMaxTimeout         time.Duration
	apiPublicURL              string
	apiDynamicPublicURLHeader string
	alwaysPaginate            bool
	handleYAML                bool
	metricsEnabled            bool
	metricsPath               string
	metricsPublicURL          string
	mux                       *mux.Router

	APIServerOptions[T]
}

type APIServerOptions[T any] struct {
	MetricsRegistry           metric.MetricsRegistry
	Routes                    []*Route
	EnrichRequest             func(r *APIRequest) (T, error)
	Description               string
	APIConfig                 config.Section
	MetricsConfig             config.Section
	CORSConfig                config.Section
	FavIcon16                 []byte
	FavIcon32                 []byte
	PanicOnMissingDescription bool
	SupportFieldRedaction     bool
	HandleYAML                bool
}

type APIServerRouteExt[T any] struct {
	JSONHandler   func(*APIRequest, T) (output interface{}, err error)
	UploadHandler func(*APIRequest, T) (output interface{}, err error)
	StreamHandler func(*APIRequest, T) (output io.ReadCloser, err error)
}

// NewAPIServer makes a new server, with the specified configuration, and
// the supplied wrapper function - which will inject
func NewAPIServer[T any](ctx context.Context, options APIServerOptions[T]) APIServer {
	as := &apiServer[T]{
		defaultFilterLimit:        options.APIConfig.GetUint64(ConfAPIDefaultFilterLimit),
		maxFilterLimit:            options.APIConfig.GetUint64(ConfAPIMaxFilterLimit),
		maxFilterSkip:             options.APIConfig.GetUint64(ConfAPIMaxFilterSkip),
		requestTimeout:            options.APIConfig.GetDuration(ConfAPIRequestTimeout),
		requestMaxTimeout:         options.APIConfig.GetDuration(ConfAPIRequestMaxTimeout),
		metricsEnabled:            options.MetricsConfig.GetBool(ConfMetricsServerEnabled),
		metricsPath:               options.MetricsConfig.GetString(ConfMetricsServerPath),
		alwaysPaginate:            options.APIConfig.GetBool(ConfAPIAlwaysPaginate),
		handleYAML:                options.HandleYAML,
		apiDynamicPublicURLHeader: options.APIConfig.GetString(ConfAPIDynamicPublicURLHeader),
		APIServerOptions:          options,
		started:                   make(chan struct{}),
	}
	if as.FavIcon16 == nil {
		as.FavIcon16 = ffLogo16
	}
	if as.FavIcon32 == nil {
		as.FavIcon32 = ffLogo16
	}
	_ = as.MetricsRegistry.NewHTTPMetricsInstrumentationsForSubsystem(
		ctx,
		APIServerMetricsSubSystemName,
		true,
		prometheus.DefBuckets,
		map[string]string{},
	)
	return as
}

// Can be called before Serve, but MUST use the background context if so
func (as *apiServer[T]) MuxRouter(ctx context.Context) *mux.Router {
	if as.mux == nil {
		as.mux = as.createMuxRouter(ctx)
	}
	return as.mux
}

// Serve is the main entry point for the API Server
func (as *apiServer[T]) Serve(ctx context.Context) (err error) {
	started := false
	defer func() {
		// Ensure we don't leave the Started() channel indefinitely in the case of an error
		if !started {
			close(as.started)
		}
	}()

	httpErrChan := make(chan error)
	metricsErrChan := make(chan error)

	apiHTTPServer, err := httpserver.NewHTTPServer(ctx, "api", as.MuxRouter(ctx), httpErrChan, as.APIConfig, as.CORSConfig, &httpserver.ServerOptions{
		MaximumRequestTimeout: as.requestMaxTimeout,
	})
	if err != nil {
		return err
	}
	as.apiPublicURL = buildPublicURL(as.APIConfig, apiHTTPServer.Addr())
	go apiHTTPServer.ServeHTTP(ctx)

	if as.metricsEnabled {
		metricsHTTPServer, err := httpserver.NewHTTPServer(ctx, "metrics", as.createMetricsMuxRouter(ctx), metricsErrChan, as.MetricsConfig, as.CORSConfig, &httpserver.ServerOptions{
			MaximumRequestTimeout: as.requestMaxTimeout,
		})
		if err != nil {
			return err
		}
		as.metricsPublicURL = buildPublicURL(as.MetricsConfig, apiHTTPServer.Addr())
		go metricsHTTPServer.ServeHTTP(ctx)
	}

	started = true
	close(as.started)
	return as.waitForServerStop(httpErrChan, metricsErrChan)
}

func (as *apiServer[T]) Started() <-chan struct{} {
	return as.started
}

func (as *apiServer[T]) APIPublicURL() string {
	return as.apiPublicURL
}

func (as *apiServer[T]) waitForServerStop(httpErrChan, metricsErrChan chan error) error {
	select {
	case err := <-httpErrChan:
		return err
	case err := <-metricsErrChan:
		return err
	}
}

func buildPublicURL(conf config.Section, a net.Addr) string {
	publicURL := conf.GetString(httpserver.HTTPConfPublicURL)
	if publicURL == "" {
		proto := "https"
		tlsConfig := conf.SubSection("tls")
		if !tlsConfig.GetBool(fftls.HTTPConfTLSEnabled) {
			proto = "http"
		}
		publicURL = fmt.Sprintf("%s://%s", proto, a.String())
	}
	return publicURL
}

func (as *apiServer[T]) routeHandler(hf *HandlerFactory, route *Route) http.HandlerFunc {
	// We extend the base ffapi functionality, with standardized DB filter support for all core resources.
	// We also pass the Orchestrator context through
	ext := route.Extensions.(*APIServerRouteExt[T])
	if route.OutputType == RouteOutputTypeStream && ext.StreamHandler != nil {
		route.StreamHandler = func(r *APIRequest) (output io.ReadCloser, err error) {
			er, err := as.EnrichRequest(r)
			if err != nil {
				return nil, err
			}
			return ext.StreamHandler(r, er)
		}
	} else {
		route.JSONHandler = func(r *APIRequest) (output interface{}, err error) {
			er, err := as.EnrichRequest(r)
			if err != nil {
				return nil, err
			}
			return ext.JSONHandler(r, er)
		}
	}

	return hf.RouteHandler(route)
}

func (as *apiServer[T]) handlerFactory() *HandlerFactory {
	return &HandlerFactory{
		DefaultRequestTimeout: as.requestTimeout,
		MaxTimeout:            as.requestMaxTimeout,
		DefaultFilterLimit:    as.defaultFilterLimit,
		MaxFilterSkip:         as.maxFilterSkip,
		MaxFilterLimit:        as.maxFilterLimit,
		SupportFieldRedaction: as.SupportFieldRedaction,
		AlwaysPaginate:        as.alwaysPaginate,
		HandleYAML:            as.handleYAML,
	}
}

func (as *apiServer[T]) createMuxRouter(ctx context.Context) *mux.Router {
	r := mux.NewRouter().UseEncodedPath()
	hf := as.handlerFactory()

	if as.metricsEnabled {
		h, _ := as.MetricsRegistry.GetHTTPMetricsInstrumentationsMiddlewareForSubsystem(ctx, APIServerMetricsSubSystemName)
		r.Use(h)
	}

	for _, route := range as.Routes {
		ce, ok := route.Extensions.(*APIServerRouteExt[T])
		if !ok {
			panic(fmt.Sprintf("invalid route extensions: %t", route.Extensions))
		}
		if ce.UploadHandler != nil {
			route.FormUploadHandler = func(r *APIRequest) (output interface{}, err error) {
				er, err := as.EnrichRequest(r)
				if err != nil {
					return nil, err
				}
				return ce.UploadHandler(r, er)
			}
		}
		if ce.JSONHandler != nil || ce.UploadHandler != nil || ce.StreamHandler != nil {
			r.HandleFunc(fmt.Sprintf("/api/v1/%s", route.Path), as.routeHandler(hf, route)).
				Methods(route.Method)
		}
	}

	oah := &OpenAPIHandlerFactory{
		BaseSwaggerGenOptions: SwaggerGenOptions{
			Title:                     as.Description,
			Version:                   "1.0",
			PanicOnMissingDescription: as.PanicOnMissingDescription,
			DefaultRequestTimeout:     as.requestTimeout,
			SupportFieldRedaction:     as.SupportFieldRedaction,
		},
		StaticPublicURL: as.apiPublicURL,
	}
	r.HandleFunc(`/api/swagger.yaml`, hf.APIWrapper(oah.OpenAPIHandler(`/api/v1`, OpenAPIFormatYAML, as.Routes)))
	r.HandleFunc(`/api/swagger.json`, hf.APIWrapper(oah.OpenAPIHandler(`/api/v1`, OpenAPIFormatJSON, as.Routes)))
	r.HandleFunc(`/api/openapi.yaml`, hf.APIWrapper(oah.OpenAPIHandler(`/api/v1`, OpenAPIFormatYAML, as.Routes)))
	r.HandleFunc(`/api/openapi.json`, hf.APIWrapper(oah.OpenAPIHandler(`/api/v1`, OpenAPIFormatJSON, as.Routes)))
	r.HandleFunc(`/api`, hf.APIWrapper(oah.SwaggerUIHandler(`/api/openapi.yaml`)))
	r.HandleFunc(`/favicon{any:.*}.png`, favIconsHandler(as.FavIcon16, as.FavIcon32))

	r.NotFoundHandler = hf.APIWrapper(as.notFoundHandler)
	return r
}

func (as *apiServer[T]) notFoundHandler(res http.ResponseWriter, req *http.Request) (status int, err error) {
	res.Header().Add("Content-Type", "application/json")
	return 404, i18n.NewError(req.Context(), i18n.Msg404NotFound)
}

func (as *apiServer[T]) createMetricsMuxRouter(ctx context.Context) *mux.Router {
	r := mux.NewRouter()
	h, err := as.MetricsRegistry.HTTPHandler(ctx, promhttp.HandlerOpts{})
	if err != nil {
		panic(err)
	}
	r.Path(as.metricsPath).Handler(h)
	return r
}
