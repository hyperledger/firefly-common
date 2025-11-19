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
	ctx    context.Context
}

// NewLogr creates a new logr.Logger backed by firefly-common's logrus wrapper
func NewLogr(ctx context.Context, name string) logr.Logger {
	return logr.New(&Sink{
		name:   name,
		ctx:    ctx,
		logger: L(ctx),
	})
}

// Init initializes the sink
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

// Info logs an info message with the given keys and values
func (l *Sink) Info(level int, msg string, keysAndValues ...interface{}) {
	logger := L(l.buildContext(keysAndValues))

	switch level {
	case 0:
		logger.Infof(msg)
	case 1:
		logger.Debugf(msg)
	default:
		logger.Tracef(msg)
	}
}

// Error logs an error message with the given keys and values
func (l *Sink) Error(err error, msg string, keysAndValues ...interface{}) {
	logger := L(l.buildContext(keysAndValues))
	if err != nil {
		logger = logger.WithError(err)
	}
	logger.Errorf(msg)
}

// WithValues adds the given keys and values to the logger
func (l *Sink) WithValues(keysAndValues ...interface{}) logr.LogSink {
	ctx := l.buildContext(keysAndValues)
	return &Sink{
		name:   l.name,
		logger: L(ctx),
		ctx:    ctx,
	}
}

// WithName adds the given name to the logger
func (l *Sink) WithName(name string) logr.LogSink {
	newName := l.name
	if len(newName) > 0 {
		newName += "."
	}
	newName += name

	return &Sink{
		name:   newName,
		logger: l.logger.WithField("logger", newName),
		ctx:    l.ctx,
	}
}

func (l *Sink) buildContext(keysAndValues []interface{}) context.Context {
	fields := make(map[string]string)
	for i := 0; i < len(keysAndValues); i += 2 {
		fields[keysAndValues[i].(string)] = keysAndValues[i+1].(string)
	}
	return WithFields(l.ctx, fields)
}
