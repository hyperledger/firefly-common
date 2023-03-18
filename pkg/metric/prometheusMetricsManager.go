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
	"regexp"

	"github.com/hyperledger/firefly-common/pkg/i18n"
	"github.com/hyperledger/firefly-common/pkg/log"
	"github.com/prometheus/client_golang/prometheus"
)

var supportedTypes = []string{"counter", "counterVec", "gauge", "gaugeVec", "histogram", "histogramVec", "summary", "summaryVec"}

type prometheusMetricsManager struct {
	namespace  string
	subsystem  string
	registry   *prometheus.Registry
	metricsMap map[string]prometheus.Collector
}

func (pmm *prometheusMetricsManager) NewCounterMetric(ctx context.Context, metricName string, helpText string) {
	pmm.registerMetrics(ctx, regInfo{
		Type:     "counter",
		Name:     metricName,
		HelpText: helpText,
	})
}
func (pmm *prometheusMetricsManager) NewCounterMetricWithLabels(ctx context.Context, metricName string, helpText string, labelNames []string) {
	pmm.registerMetrics(ctx, regInfo{
		Type:       "counterVec",
		Name:       metricName,
		HelpText:   helpText,
		LabelNames: labelNames,
	})
}
func (pmm *prometheusMetricsManager) NewGaugeMetric(ctx context.Context, metricName string, helpText string) {
	pmm.registerMetrics(ctx, regInfo{
		Type:     "gauge",
		Name:     metricName,
		HelpText: helpText,
	})
}
func (pmm *prometheusMetricsManager) NewGaugeMetricWithLabels(ctx context.Context, metricName string, helpText string, labelNames []string) {
	pmm.registerMetrics(ctx, regInfo{
		Type:       "gaugeVec",
		Name:       metricName,
		HelpText:   helpText,
		LabelNames: labelNames,
	})
}
func (pmm *prometheusMetricsManager) NewHistogramMetric(ctx context.Context, metricName string, helpText string, buckets []float64) {
	pmm.registerMetrics(ctx, regInfo{
		Type:     "histogram",
		Name:     metricName,
		HelpText: helpText,
		Buckets:  buckets,
	})
}
func (pmm *prometheusMetricsManager) NewHistogramMetricWithLabels(ctx context.Context, metricName string, helpText string, buckets []float64, labelNames []string) {
	pmm.registerMetrics(ctx, regInfo{
		Type:       "histogramVec",
		Name:       metricName,
		HelpText:   helpText,
		Buckets:    buckets,
		LabelNames: labelNames,
	})
}
func (pmm *prometheusMetricsManager) NewSummaryMetric(ctx context.Context, metricName string, helpText string) {
	pmm.registerMetrics(ctx, regInfo{
		Type:     "summary",
		Name:     metricName,
		HelpText: helpText,
	})
}
func (pmm *prometheusMetricsManager) NewSummaryMetricWithLabels(ctx context.Context, metricName string, helpText string, labelNames []string) {
	pmm.registerMetrics(ctx, regInfo{
		Type:       "summaryVec",
		Name:       metricName,
		HelpText:   helpText,
		LabelNames: labelNames,
	})
}

type regInfo struct {
	Type       string
	Name       string
	HelpText   string
	LabelNames []string  // should only be provided for metrics types with vectors
	Buckets    []float64 // only applicable for histogram
}

func (pmm *prometheusMetricsManager) registerMetrics(ctx context.Context, mr regInfo) {

	nameRegex := regexp.MustCompile(allowedNameStringRegex)
	isValidNameString := nameRegex.MatchString(mr.Name)
	if !isValidNameString {
		err := i18n.NewError(ctx, i18n.MsgMetricsInvalidName, mr.Name)
		log.L(ctx).Warnf("Failed to initialize metric %s due to error: %s", mr.Name, err.Error())
		return
	}

	for _, typeName := range supportedTypes {
		if _, ok := pmm.metricsMap[mr.Name+"_"+typeName]; ok {
			err := i18n.NewError(ctx, i18n.MsgMetricsDuplicateName, mr.Name)
			log.L(ctx).Warnf("Failed to initialize metric %s due to error: %s", mr.Name, err.Error())
			return

		}
	}
	internalMapIndex := mr.Name + "_" + mr.Type
	if mr.HelpText == "" {
		err := i18n.NewError(ctx, i18n.MsgMetricsHelpTextMissing)
		log.L(ctx).Warnf("Failed to initialize metric %s due to error: %s", mr.Name, err.Error())
		return
	}
	switch mr.Type {
	case "counter":
		pmm.metricsMap[internalMapIndex] = prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: pmm.namespace,
			Subsystem: pmm.subsystem,
			Name:      mr.Name,
			Help:      mr.HelpText,
		})
	case "counterVec":
		pmm.metricsMap[internalMapIndex] = prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: pmm.namespace,
			Subsystem: pmm.subsystem,
			Name:      mr.Name,
			Help:      mr.HelpText,
		}, mr.LabelNames)
	case "gauge":
		pmm.metricsMap[internalMapIndex] = prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: pmm.namespace,
			Subsystem: pmm.subsystem,
			Name:      mr.Name,
			Help:      mr.HelpText,
		})
	case "gaugeVec":
		pmm.metricsMap[internalMapIndex] = prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: pmm.namespace,
			Subsystem: pmm.subsystem,
			Name:      mr.Name,
			Help:      mr.HelpText,
		}, mr.LabelNames)
	case "histogram":
		pmm.metricsMap[internalMapIndex] = prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: pmm.namespace,
			Subsystem: pmm.subsystem,
			Name:      mr.Name,
			Help:      mr.HelpText,
			Buckets:   mr.Buckets,
		})
	case "histogramVec":
		pmm.metricsMap[internalMapIndex] = prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: pmm.namespace,
			Subsystem: pmm.subsystem,
			Name:      mr.Name,
			Help:      mr.HelpText,
			Buckets:   mr.Buckets,
		}, mr.LabelNames)
	case "summary":
		pmm.metricsMap[internalMapIndex] = prometheus.NewSummary(prometheus.SummaryOpts{
			Namespace: pmm.namespace,
			Subsystem: pmm.subsystem,
			Name:      mr.Name,
			Help:      mr.HelpText,
		})
	case "summaryVec":
		pmm.metricsMap[internalMapIndex] = prometheus.NewSummaryVec(prometheus.SummaryOpts{
			Namespace: pmm.namespace,
			Subsystem: pmm.subsystem,
			Name:      mr.Name,
			Help:      mr.HelpText,
		}, mr.LabelNames)
	}
	pmm.registry.MustRegister(pmm.metricsMap[internalMapIndex])
}

func (pmm *prometheusMetricsManager) SetGaugeMetric(ctx context.Context, metricName string, number float64) {
	collector, ok := pmm.metricsMap[metricName+"_gauge"]
	if !ok {
		log.L(ctx).Warnf("Transaction handler metric with name: '%s' and type: '%s' is not found", metricName, "gauge")
	} else {
		collector.(prometheus.Gauge).Set(number)
	}
}
func (pmm *prometheusMetricsManager) SetGaugeMetricWithLabels(ctx context.Context, metricName string, number float64, labels map[string]string) {
	collector, ok := pmm.metricsMap[metricName+"_gaugeVec"]
	if !ok {
		log.L(ctx).Warnf("Transaction handler metric with name: '%s' and type: '%s' is not found", metricName, "gaugeVec")
	} else {
		collector.(*prometheus.GaugeVec).With(labels).Set(number)
	}
}

func (pmm *prometheusMetricsManager) IncCounterMetric(ctx context.Context, metricName string) {
	collector, ok := pmm.metricsMap[metricName+"_counter"]
	if !ok {
		log.L(ctx).Warnf("Transaction handler metric with name: '%s' and type: '%s' is not found", metricName, "counter")
	} else {
		collector.(prometheus.Counter).Inc()
	}
}

func (pmm *prometheusMetricsManager) IncCounterMetricWithLabels(ctx context.Context, metricName string, labels map[string]string) {
	collector, ok := pmm.metricsMap[metricName+"_counterVec"]
	if !ok {
		log.L(ctx).Warnf("Transaction handler metric with name: '%s' and type: '%s' is not found", metricName, "counterVec")
	} else {
		collector.(*prometheus.CounterVec).With(labels).Inc()
	}
}

func (pmm *prometheusMetricsManager) ObserveHistogramMetric(ctx context.Context, metricName string, number float64) {
	collector, ok := pmm.metricsMap[metricName+"_histogram"]
	if !ok {
		log.L(ctx).Warnf("Transaction handler metric with name: '%s' and type: '%s' is not found", metricName, "histogram")
	} else {
		collector.(prometheus.Histogram).Observe(number)
	}
}

func (pmm *prometheusMetricsManager) ObserveHistogramMetricWithLabels(ctx context.Context, metricName string, number float64, labels map[string]string) {
	collector, ok := pmm.metricsMap[metricName+"_histogramVec"]
	if !ok {
		log.L(ctx).Warnf("Transaction handler metric with name: '%s' and type: '%s' is not found", metricName, "histogramVec")
	} else {
		collector.(*prometheus.HistogramVec).With(labels).Observe(number)
	}
}

func (pmm *prometheusMetricsManager) ObserveSummaryMetric(ctx context.Context, metricName string, number float64) {
	collector, ok := pmm.metricsMap[metricName+"_summary"]
	if !ok {
		log.L(ctx).Warnf("Transaction handler metric with name: '%s' and type: '%s' is not found", metricName, "summary")
	} else {
		collector.(prometheus.Summary).Observe(number)
	}
}

func (pmm *prometheusMetricsManager) ObserveSummaryMetricWithLabels(ctx context.Context, metricName string, number float64, labels map[string]string) {
	collector, ok := pmm.metricsMap[metricName+"_summaryVec"]
	if !ok {
		log.L(ctx).Warnf("Transaction handler metric with name: '%s' and type: '%s' is not found", metricName, "summaryVec")
	} else {
		collector.(*prometheus.SummaryVec).With(labels).Observe(number)
	}
}
