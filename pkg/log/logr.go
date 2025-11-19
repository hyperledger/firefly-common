// Copyright © 2022 - 2025 Kaleido, Inc.
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

package log

import (
	"context"

	"github.com/go-logr/logr"
	"github.com/sirupsen/logrus"
)

// logr.LogSink duck type backed by firefly-common's logrus wrapper
type Sink struct {
	name   string
	logger *logrus.Entry
	values []interface{}
}

func NewLogr(ctx context.Context, name string) logr.Logger {
	return logr.New(&Sink{
		name:   name,
		logger: L(ctx), // or log.NewLogger() depending on your setup
		values: []interface{}{},
	})
}

func (l *Sink) Init(_ logr.RuntimeInfo) {
	// Optional: store callDepth if needed
}

func (l *Sink) Enabled(level int) bool {
	// Map logr V-levels to ff-common levels
	// logr: V(0)=info, V(1)=debug, V(2+)=trace
	switch level {
	case 0:
		return l.logger.Level >= logrus.InfoLevel
	case 1:
		return l.logger.Level >= logrus.DebugLevel
	default:
		return l.logger.Level >= logrus.TraceLevel
	}
}

func (l *Sink) Info(level int, msg string, keysAndValues ...interface{}) {
	fields := l.mergeKeysAndValues(keysAndValues)

	switch level {
	case 0:
		l.logger.Infof(msg, fields...)
	case 1:
		l.logger.Debugf(msg, fields...)
	default:
		l.logger.Tracef(msg, fields...)
	}
}

func (l *Sink) Error(err error, msg string, keysAndValues ...interface{}) {
	fields := l.mergeKeysAndValues(keysAndValues)
	if err != nil {
		fields = append(fields, "error", err)
	}
	l.logger.Errorf(msg, fields...)
}

func (l *Sink) WithValues(keysAndValues ...interface{}) logr.LogSink {
	return &Sink{
		name:   l.name,
		logger: l.logger,
		values: append(l.values, keysAndValues...),
	}
}

func (l *Sink) WithName(name string) logr.LogSink {
	newName := l.name
	if len(newName) > 0 {
		newName += "."
	}
	newName += name

	return &Sink{
		name:   newName,
		logger: l.logger.WithField("logger", newName),
		values: l.values,
	}
}

func (l *Sink) mergeKeysAndValues(keysAndValues []interface{}) []interface{} {
	result := make([]interface{}, 0, len(l.values)+len(keysAndValues)+2)
	if l.name != "" {
		result = append(result, "logger", l.name)
	}
	result = append(result, l.values...)
	result = append(result, keysAndValues...)
	return result
}
