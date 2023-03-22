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
	mr := NewPrometheusMetricsRegistry("test")
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
	mr := NewPrometheusMetricsRegistry("test")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mm, err := mr.NewMetricsManagerForSubsystem(ctx, "tm")
	assert.NoError(t, err)

	mm.NewCounterMetric(ctx, "tx_request", "Transactions requests handled", false)
	mm.NewCounterMetricWithLabels(ctx, "tx_process", "Transaction processed", []string{"status"}, false)
	mm.NewGaugeMetric(ctx, "tx_stalled", "Transactions that are stuck in a loop", false)
	mm.NewGaugeMetricWithLabels(ctx, "tx_inflight", "Transactions that are in flight", []string{"stage"}, false)
	mm.NewHistogramMetric(ctx, "tx_timeout_seconds", "Duration of timed out transactions", []float64{}, false)
	mm.NewHistogramMetricWithLabels(ctx, "tx_stage_seconds", "Duration of each transaction stage", []float64{}, []string{"stage"}, false)
	mm.NewSummaryMetric(ctx, "tx_request_bytes", "Request size of timed out transactions", false)
	mm.NewSummaryMetricWithLabels(ctx, "tx_retry_bytes", "Retry request size of each transaction stage", []string{"stage"}, false)

	mm.IncCounterMetric(ctx, "tx_request", nil)
	mm.IncCounterMetricWithLabels(ctx, "tx_process", map[string]string{"status": "success"}, nil)
	mm.SetGaugeMetric(ctx, "tx_stalled", 2, nil)
	mm.SetGaugeMetricWithLabels(ctx, "tx_inflight", 2, map[string]string{"stage": "singing:)"}, nil)
	mm.ObserveHistogramMetric(ctx, "tx_timeout_seconds", 2000, nil)
	mm.ObserveHistogramMetricWithLabels(ctx, "tx_stage_seconds", 2000, map[string]string{"stage": "singing:)"}, nil)
	mm.ObserveHistogramMetricWithLabels(ctx, "tx_stage_seconds", 2000, map[string]string{}, nil) // no label provided
	mm.ObserveSummaryMetric(ctx, "tx_request_bytes", 2000, nil)
	mm.ObserveSummaryMetricWithLabels(ctx, "tx_retry_bytes", 2000, map[string]string{"stage": "singing:)"}, nil)
}

func TestMetricsManagerWithDefaultLabels(t *testing.T) {
	mr := NewPrometheusMetricsRegistry("test")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mm, err := mr.NewMetricsManagerForSubsystem(ctx, "tm")
	assert.NoError(t, err)

	mm.NewCounterMetric(ctx, "tx_request", "Transactions requests handled", true)
	mm.NewCounterMetricWithLabels(ctx, "tx_process", "Transaction processed", []string{"status"}, true)
	mm.NewGaugeMetric(ctx, "tx_stalled", "Transactions that are stuck in a loop", true)
	mm.NewGaugeMetricWithLabels(ctx, "tx_inflight", "Transactions that are in flight", []string{"stage"}, true)
	mm.NewHistogramMetric(ctx, "tx_timeout_seconds", "Duration of timed out transactions", []float64{}, true)
	mm.NewHistogramMetricWithLabels(ctx, "tx_stage_seconds", "Duration of each transaction stage", []float64{}, []string{"stage"}, true)
	mm.NewSummaryMetric(ctx, "tx_request_bytes", "Request size of timed out transactions", true)
	mm.NewSummaryMetricWithLabels(ctx, "tx_retry_bytes", "Retry request size of each transaction stage", []string{"stage"}, true)

	// provides no values for the default labels
	mm.IncCounterMetric(ctx, "tx_request", nil)
	mm.IncCounterMetricWithLabels(ctx, "tx_process", map[string]string{"status": "success"}, nil)
	mm.SetGaugeMetric(ctx, "tx_stalled", 2, nil)
	mm.SetGaugeMetricWithLabels(ctx, "tx_inflight", 2, map[string]string{"stage": "singing:)"}, nil)
	mm.ObserveHistogramMetric(ctx, "tx_timeout_seconds", 2000, nil)
	mm.ObserveHistogramMetricWithLabels(ctx, "tx_stage_seconds", 2000, map[string]string{"stage": "singing:)"}, nil)
	mm.ObserveSummaryMetric(ctx, "tx_request_bytes", 2000, nil)
	mm.ObserveSummaryMetricWithLabels(ctx, "tx_retry_bytes", 2000, map[string]string{"stage": "singing:)"}, nil)

	mm.IncCounterMetric(ctx, "tx_request", &FireflyDefaultLabels{Namespace: "ns_1"})
	mm.IncCounterMetricWithLabels(ctx, "tx_process", map[string]string{"status": "success"}, &FireflyDefaultLabels{Namespace: "ns_1"})
	mm.SetGaugeMetric(ctx, "tx_stalled", 2, &FireflyDefaultLabels{Namespace: "ns_1"})
	mm.SetGaugeMetricWithLabels(ctx, "tx_inflight", 2, map[string]string{"stage": "singing:)"}, &FireflyDefaultLabels{Namespace: "ns_1"})
	mm.ObserveHistogramMetric(ctx, "tx_timeout_seconds", 2000, &FireflyDefaultLabels{Namespace: "ns_1"})
	mm.ObserveHistogramMetricWithLabels(ctx, "tx_stage_seconds", 2000, map[string]string{"stage": "singing:)"}, &FireflyDefaultLabels{Namespace: "ns_1"})
	mm.ObserveHistogramMetricWithLabels(ctx, "tx_stage_seconds", 2000, map[string]string{}, nil) // no label provided
	mm.ObserveSummaryMetric(ctx, "tx_request_bytes", 2000, &FireflyDefaultLabels{Namespace: "ns_1"})
	mm.ObserveSummaryMetricWithLabels(ctx, "tx_retry_bytes", 2000, map[string]string{"stage": "singing:)"}, &FireflyDefaultLabels{Namespace: "ns_1"})

}

func TestMetricsManagerErrors(t *testing.T) {
	mr := NewPrometheusMetricsRegistry("test")
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
	mm.NewCounterMetric(ctx, "tx Invalid Name", "Invalid name", false)
	mm.NewGaugeMetric(ctx, "tx_duplicate", "Duplicate registration", false)
	mm.NewGaugeMetric(ctx, "tx_duplicate", "Duplicate registration", false)
	mm.NewGaugeMetric(ctx, "tx_no_help_text", "", false)
	mm.NewGaugeMetricWithLabels(ctx, "tx_inflight_invalid_labels", "Transactions that are in flight", []string{"ff_stage"}, true)

	// swallow emit errors, none of the metrics below are registered
	mm.IncCounterMetric(ctx, "tx_not_exist", nil)
	mm.IncCounterMetricWithLabels(ctx, "tx_not_exist", map[string]string{"status": "success"}, nil)
	mm.SetGaugeMetric(ctx, "tx_not_exist", 2, nil)
	mm.SetGaugeMetricWithLabels(ctx, "tx_not_exist", 2, map[string]string{"stage": "singing:)"}, nil)
	mm.ObserveHistogramMetric(ctx, "tx_not_exist", 2000, nil)
	mm.ObserveHistogramMetricWithLabels(ctx, "tx_not_exist", 2000, map[string]string{"stage": "singing:)"}, nil)
	mm.ObserveSummaryMetric(ctx, "tx_not_exist", 2000, nil)
	mm.ObserveSummaryMetricWithLabels(ctx, "tx_not_exist", 2000, map[string]string{"stage": "singing:)"}, nil)
	mm.SetGaugeMetricWithLabels(ctx, "tx_inflight_invalid_labels", 2, map[string]string{"ff_stage": "singing:)"}, nil)
}

func TestHTTPMetricsInstrumentations(t *testing.T) {
	mr := NewPrometheusMetricsRegistry("test")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := mr.NewHTTPMetricsInstrumentationsForSubsystem(ctx, "api_server", true, []float64{}, nil)
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
