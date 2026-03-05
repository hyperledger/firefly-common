// Copyright © 2026 Kaleido, Inc.
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

package dbsql

import (
	"context"
	"database/sql"

	"github.com/hyperledger/firefly-common/pkg/metric"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

type metricsOptions struct {
	dbName string
}

func (opts *metricsOptions) apply(options ...MetricsOption) {
	for _, option := range options {
		option(opts)
	}
}

type MetricsOption func(opts *metricsOptions)

// WithDBName sets the name of the database label for the metrics collector.
func WithDBName(dbName string) MetricsOption {
	return func(opts *metricsOptions) {
		opts.dbName = dbName
	}
}

// EnableDBMetrics registers a prometheus collector for the given database connection.
func EnableDBMetrics(_ context.Context, metricsRegistry metric.MetricsRegistry, db *sql.DB, options ...MetricsOption) {
	opts := &metricsOptions{}
	opts.apply(options...)

	if opts.dbName == "" {
		opts.dbName = "default"
	}

	metricsRegistry.MustRegisterCollector(collectors.NewDBStatsCollector(db, opts.dbName))
}
