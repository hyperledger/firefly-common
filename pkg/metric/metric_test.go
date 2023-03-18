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
	"testing"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/stretchr/testify/assert"
)

func TestMetricsRegistry(t *testing.T) {
	mr := NewPrometheusMetricsRegistry()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, err := mr.HTTPHandler(ctx, promhttp.HandlerOpts{})
	assert.Error(t, err)
	assert.Regexp(t, "FF00200", err)
	_, err = mr.NewMetricsManagerForSubsystem(ctx, "test")
	assert.NoError(t, err)
	// successfully returns the http handler once a subsystem is registered
	httpHandler, err := mr.HTTPHandler(ctx, promhttp.HandlerOpts{})
	assert.NoError(t, err)
	assert.NotNil(t, httpHandler)
}

func TestMetricsManager(t *testing.T) {
	mr := NewPrometheusMetricsRegistry()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mm, err := mr.NewMetricsManagerForSubsystem(ctx, "tm")
	assert.NoError(t, err)

	mm.NewCounterMetric(ctx, "tx_request", "Transactions requests handled")
	mm.NewCounterMetricWithLabels(ctx, "tx_process", "Transaction processed", []string{"status"})
	mm.NewGaugeMetric(ctx, "tx_stalled", "Transactions that are stuck in a loop")
	mm.NewGaugeMetricWithLabels(ctx, "tx_inflight", "Transactions that are in flight", []string{"stage"})
	mm.NewHistogramMetric(ctx, "tx_timeout_seconds", "Duration of timed out transactions", []float64{})
	mm.NewHistogramMetricWithLabels(ctx, "tx_stage_seconds", "Duration of each transaction stage", []float64{}, []string{"stage"})
	mm.NewSummaryMetric(ctx, "tx_request_bytes", "Request size of timed out transactions")
	mm.NewSummaryMetricWithLabels(ctx, "tx_retry_bytes", "Retry request size of each transaction stage", []string{"stage"})

	mm.IncCounterMetric(ctx, "tx_request")
	mm.IncCounterMetricWithLabels(ctx, "tx_process", map[string]string{"status": "success"})
	mm.SetGaugeMetric(ctx, "tx_stalled", 2)
	mm.SetGaugeMetricWithLabels(ctx, "tx_inflight", 2, map[string]string{"stage": "singing:)"})
	mm.ObserveHistogramMetric(ctx, "tx_timeout_seconds", 2000)
	mm.ObserveHistogramMetricWithLabels(ctx, "tx_stage_seconds", 2000, map[string]string{"stage": "singing:)"})
	mm.ObserveSummaryMetric(ctx, "tx_request_bytes", 2000)
	mm.ObserveSummaryMetricWithLabels(ctx, "tx_retry_bytes", 2000, map[string]string{"stage": "singing:)"})
}

func TestMetricsManagerErrors(t *testing.T) {
	mr := NewPrometheusMetricsRegistry()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mm, err := mr.NewMetricsManagerForSubsystem(ctx, "tm")
	assert.NoError(t, err)
	_, err = mr.NewMetricsManagerForSubsystem(ctx, "tm")
	assert.Error(t, err)
	assert.Regexp(t, "FF00196", err)
	// check a subsystem can only have a metrics manager or a default http metrics instrumentations
	err = mr.NewHTTPMetricsInstrumentationsForSubsystem(ctx, "tm", true, []float64{}, map[string]string{})
	assert.Error(t, err)
	assert.Regexp(t, "FF00196", err)
	_, err = mr.NewMetricsManagerForSubsystem(ctx, "Invalid name")
	assert.Error(t, err)
	assert.Regexp(t, "FF00195", err)

	// swallow init errors
	mm.NewCounterMetric(ctx, "txInvalidName", "Invalid name")
	mm.NewGaugeMetric(ctx, "tx_duplicate", "Duplicate registration")
	mm.NewGaugeMetric(ctx, "tx_duplicate", "Duplicate registration")
	mm.NewGaugeMetric(ctx, "tx_no_help_text", "")

	// swallow emit errors, none of the metrics below are registered
	mm.IncCounterMetric(ctx, "tx_not_exist")
	mm.IncCounterMetricWithLabels(ctx, "tx_not_exist", map[string]string{"status": "success"})
	mm.SetGaugeMetric(ctx, "tx_not_exist", 2)
	mm.SetGaugeMetricWithLabels(ctx, "tx_not_exist", 2, map[string]string{"stage": "singing:)"})
	mm.ObserveHistogramMetric(ctx, "tx_not_exist", 2000)
	mm.ObserveHistogramMetricWithLabels(ctx, "tx_not_exist", 2000, map[string]string{"stage": "singing:)"})
	mm.ObserveSummaryMetric(ctx, "tx_not_exist", 2000)
	mm.ObserveSummaryMetricWithLabels(ctx, "tx_not_exist", 2000, map[string]string{"stage": "singing:)"})
}

func TestHTTPMetricsInstrumentations(t *testing.T) {
	mr := NewPrometheusMetricsRegistry()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := mr.NewHTTPMetricsInstrumentationsForSubsystem(ctx, "api_server", true, []float64{}, map[string]string{})
	assert.NoError(t, err)
	err = mr.NewHTTPMetricsInstrumentationsForSubsystem(ctx, "api_server", true, []float64{}, map[string]string{})
	assert.Error(t, err)
	assert.Regexp(t, "FF00196", err)
	_, err = mr.NewMetricsManagerForSubsystem(ctx, "api_server")
	assert.Error(t, err)
	assert.Regexp(t, "FF00196", err)
	// check a subsystem can only have a metrics manager or a default http metrics instrumentations
	err = mr.NewHTTPMetricsInstrumentationsForSubsystem(ctx, "_invalid_name_", true, []float64{}, map[string]string{})
	assert.Error(t, err)
	assert.Regexp(t, "FF00195", err)

	// check no http metrics instrumentation found for a subsystem with metrics manager registered
	_, err = mr.NewMetricsManagerForSubsystem(ctx, "tm")
	assert.NoError(t, err)
	_, err = mr.GetHTTPMetricsInstrumentationsMiddlewareForSubsystem(ctx, "tm")
	assert.Error(t, err)
	assert.Regexp(t, "FF00201", err)

	httpMiddleware, err := mr.GetHTTPMetricsInstrumentationsMiddlewareForSubsystem(ctx, "api_server")
	assert.NoError(t, err)
	assert.NotNil(t, httpMiddleware)
}
