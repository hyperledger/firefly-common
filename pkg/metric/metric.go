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

package metric

import (
	"context"
	"net/http"
	"regexp"
	"sync"

	"github.com/hyperledger/firefly-common/pkg/i18n"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	muxprom "gitlab.com/hfuss/mux-prometheus/pkg/middleware"
)

var allowedNameStringRegex = `^[a-z]+[a-z_]*[a-z]$`
var ffMetricsPrefix = "ff"

// MetricsRegistry contains all metrics defined in a micro-service.
//   - All metrics will have the default "ff" prefix
//   - Metrics are defined in subsystems, a subsystem can only use one of the two options for creating metrics:
//     1. create a metrics manager to defined custom metrics manually
//     2. use out-of-box predefined metrics for a common resource type (e.g. HTTP request)
type MetricsRegistry interface {
	// NewMetricsManagerForSubsystem returns a MetricsManager which can be used to add custom metrics
	NewMetricsManagerForSubsystem(ctx context.Context, subsystem string) (MetricsManager, error)

	// HTTPHandler returns the HTTP handler of this metrics registry
	HTTPHandler(ctx context.Context, handlerOpts promhttp.HandlerOpts) (http.Handler, error)

	// Predefined HTTP Metrics  Instrumentations
	// NewHTTPMetricsInstrumentationsForSubsystem adds predefined HTTP metrics to a subsystem
	NewHTTPMetricsInstrumentationsForSubsystem(ctx context.Context, subsystem string, useRouteTemplate bool, reqDurationBuckets []float64, labels map[string]string) error
	// GetHTTPMetricsInstrumentationsMiddlewareForSubsystem returns the HTTP middleware of a subsystem that used predefined HTTP metrics
	GetHTTPMetricsInstrumentationsMiddlewareForSubsystem(ctx context.Context, subsystem string) (func(next http.Handler) http.Handler, error)
}

// MetricsManager is used to defined and emit metrics in a subsystem
type MetricsManager interface {
	// functions for defining metrics
	NewCounterMetric(ctx context.Context, metricName string, helpText string)
	NewCounterMetricWithLabels(ctx context.Context, metricName string, helpText string, labelNames []string)
	NewGaugeMetric(ctx context.Context, metricName string, helpText string)
	NewGaugeMetricWithLabels(ctx context.Context, metricName string, helpText string, labelNames []string)
	NewHistogramMetric(ctx context.Context, metricName string, helpText string, buckets []float64)
	NewHistogramMetricWithLabels(ctx context.Context, metricName string, helpText string, buckets []float64, labelNames []string)
	NewSummaryMetric(ctx context.Context, metricName string, helpText string)
	NewSummaryMetricWithLabels(ctx context.Context, metricName string, helpText string, labelNames []string)

	// functions for emitting metrics
	SetGaugeMetric(ctx context.Context, metricName string, number float64)
	SetGaugeMetricWithLabels(ctx context.Context, metricName string, number float64, labels map[string]string)
	IncCounterMetric(ctx context.Context, metricName string)
	IncCounterMetricWithLabels(ctx context.Context, metricName string, labels map[string]string)
	ObserveHistogramMetric(ctx context.Context, metricName string, number float64)
	ObserveHistogramMetricWithLabels(ctx context.Context, metricName string, number float64, labels map[string]string)
	ObserveSummaryMetric(ctx context.Context, metricName string, number float64)
	ObserveSummaryMetricWithLabels(ctx context.Context, metricName string, number float64, labels map[string]string)
}

func NewPrometheusMetricsRegistry() MetricsRegistry {
	return &prometheusMetricsRegistry{
		namespace:                      ffMetricsPrefix,
		registry:                       prometheus.NewRegistry(),
		managerMap:                     make(map[string]MetricsManager),
		httpMetricsInstrumentationsMap: make(map[string]*muxprom.Instrumentation),
	}
}

type prometheusMetricsRegistry struct {
	regMux                         sync.Mutex
	registry                       *prometheus.Registry
	namespace                      string
	managerMap                     map[string]MetricsManager
	httpMetricsInstrumentationsMap map[string]*muxprom.Instrumentation
}

// Custom prometheus metrics manager
func (pmr *prometheusMetricsRegistry) NewMetricsManagerForSubsystem(ctx context.Context, subsystem string) (MetricsManager, error) {
	pmr.regMux.Lock()
	defer pmr.regMux.Unlock()
	nameRegex := regexp.MustCompile(allowedNameStringRegex)
	isValidNameString := nameRegex.MatchString(subsystem)
	if !isValidNameString {
		return nil, i18n.NewError(ctx, i18n.MsgMetricsInvalidSubsystemName, subsystem)
	}

	if _, ok := pmr.managerMap[subsystem]; ok {
		return nil, i18n.NewError(ctx, i18n.MsgMetricsDuplicateSubsystemName, subsystem)
	}
	if _, ok := pmr.httpMetricsInstrumentationsMap[subsystem]; ok {
		return nil, i18n.NewError(ctx, i18n.MsgMetricsDuplicateSubsystemName, subsystem)
	}

	pmr.managerMap[subsystem] = &prometheusMetricsManager{
		namespace:  pmr.namespace,
		subsystem:  subsystem,
		registry:   pmr.registry,
		metricsMap: make(map[string]prometheus.Collector),
	}
	return pmr.managerMap[subsystem], nil
}

// Predefined metrics for http router
func (pmr *prometheusMetricsRegistry) NewHTTPMetricsInstrumentationsForSubsystem(ctx context.Context, subsystem string, useRouteTemplate bool, reqDurationBuckets []float64, labels map[string]string) error {
	pmr.regMux.Lock()
	defer pmr.regMux.Unlock()
	nameRegex := regexp.MustCompile(allowedNameStringRegex)
	isValidNameString := nameRegex.MatchString(subsystem)
	if !isValidNameString {
		return i18n.NewError(ctx, i18n.MsgMetricsInvalidSubsystemName, subsystem)
	}
	if _, ok := pmr.managerMap[subsystem]; ok {
		return i18n.NewError(ctx, i18n.MsgMetricsDuplicateSubsystemName, subsystem)
	}
	if _, ok := pmr.httpMetricsInstrumentationsMap[subsystem]; ok {
		return i18n.NewError(ctx, i18n.MsgMetricsDuplicateSubsystemName, subsystem)
	}
	httpInstrumentation := muxprom.NewCustomInstrumentation(
		useRouteTemplate,
		pmr.namespace,
		subsystem,
		reqDurationBuckets,
		labels,
		pmr.registry,
	)
	pmr.httpMetricsInstrumentationsMap[subsystem] = httpInstrumentation
	return nil
}

func (pmr *prometheusMetricsRegistry) HTTPHandler(ctx context.Context, handlerOpts promhttp.HandlerOpts) (http.Handler, error) {
	if len(pmr.managerMap) == 0 && len(pmr.httpMetricsInstrumentationsMap) == 0 {
		return nil, i18n.NewError(ctx, i18n.MsgMetricsEmptyRegistry)
	}
	return promhttp.InstrumentMetricHandler(pmr.registry, promhttp.HandlerFor(pmr.registry, handlerOpts)), nil
}

func (pmr *prometheusMetricsRegistry) GetHTTPMetricsInstrumentationsMiddlewareForSubsystem(ctx context.Context, subsystem string) (func(next http.Handler) http.Handler, error) {
	httpInstrumentation, ok := pmr.httpMetricsInstrumentationsMap[subsystem]
	if !ok {
		return nil, i18n.NewError(ctx, i18n.MsgMetricsSubsystemHTTPInstrumentationNotFound)
	}
	return httpInstrumentation.Middleware, nil
}
