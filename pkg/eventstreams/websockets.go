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

package eventstreams

import (
	"context"
	"database/sql/driver"

	"github.com/hyperledger/firefly-common/pkg/fftypes"
	"github.com/hyperledger/firefly-common/pkg/i18n"
	"github.com/hyperledger/firefly-common/pkg/log"
	"github.com/hyperledger/firefly-common/pkg/wsserver"
)

type DistributionMode = fftypes.FFEnum

var (
	DistributionModeBroadcast   = fftypes.FFEnumValue("distmode", "broadcast")
	DistributionModeLoadBalance = fftypes.FFEnumValue("distmode", "load_balance")
)

type WebSocketConfig struct {
	DistributionMode *DistributionMode `ffstruct:"wsconfig" json:"distributionMode,omitempty"`
}

type WebSocketEventBatch struct {
	BatchNumber int64              `ffstruct:"wsevent" json:"batchNumber"`
	Events      []*fftypes.JSONAny `ffstruct:"wsevent" json:"events"`
}

// Store in DB as JSON
func (wc *WebSocketConfig) Scan(src interface{}) error {
	return fftypes.JSONScan(src, wc)
}

// Store in DB as JSON
func (wc *WebSocketConfig) Value() (driver.Value, error) {
	if wc == nil {
		return nil, nil
	}
	return fftypes.JSONValue(wc)
}

func (wc *WebSocketConfig) Validate(ctx context.Context, defaults *ConfigWebsocketDefaults) error {

	if wc.DistributionMode == nil {
		def := defaults.DefaultDistributionMode
		wc.DistributionMode = &def
	}

	switch *wc.DistributionMode {
	case DistributionModeLoadBalance, DistributionModeBroadcast:
	default:
		return i18n.NewError(ctx, i18n.MsgInvalidDistributionMode, *wc.DistributionMode)
	}

	return nil
}

type webSocketAction struct {
	topic      string
	spec       *WebSocketConfig
	wsChannels wsserver.WebSocketChannels
}

func newWebSocketAction(wsChannels wsserver.WebSocketChannels, spec *WebSocketConfig, topic string) *webSocketAction {
	return &webSocketAction{
		spec:       spec,
		wsChannels: wsChannels,
		topic:      topic,
	}
}

func (w *webSocketAction) attemptDispatch(ctx context.Context, batchNumber int64, attempt int, events []*fftypes.JSONAny) error {
	var err error

	// Get a blocking channel to send and receive on our chosen namespace
	sender, broadcaster, receiver := w.wsChannels.GetChannels(w.topic)

	var channel chan<- interface{}
	isBroadcast := *w.spec.DistributionMode == DistributionModeBroadcast
	if isBroadcast {
		channel = broadcaster
	} else {
		channel = sender
	}

	// Send the batch of events
	select {
	case channel <- &WebSocketEventBatch{
		BatchNumber: batchNumber,
		Events:      events,
	}:
		break
	case <-ctx.Done():
		err = i18n.NewError(ctx, i18n.MsgWebSocketInterruptedSend)
	}

	if err == nil && !isBroadcast {
		log.L(ctx).Infof("Batch %d dispatched (len=%d,attempt=%d)", batchNumber, len(events), attempt)
		err = w.waitForAck(ctx, receiver, batchNumber)
	}

	// Pass back any exception due
	if err != nil {
		log.L(ctx).Infof("WebSocket event batch %d delivery failed (len=%d,attempt=%d): %s", batchNumber, len(events), attempt, err)
		return err
	}
	log.L(ctx).Infof("WebSocket event batch %d complete (len=%d,attempt=%d)", batchNumber, len(events), attempt)
	return nil
}

func (w *webSocketAction) waitForAck(ctx context.Context, receiver <-chan *wsserver.WebSocketCommandMessageOrError, batchNumber int64) error {
	// Wait for the next ack or exception
	for {
		select {
		case msgOrErr := <-receiver:
			if msgOrErr.Err != nil {
				// If we get an error, we have to assume the other side did not receive this batch, and send it again
				return msgOrErr.Err
			}
			if msgOrErr.Msg.BatchNumber != batchNumber {
				log.L(ctx).Infof("Discarding ack for batch %d (awaiting %d)", msgOrErr.Msg.BatchNumber, batchNumber)
				continue
			}
			log.L(ctx).Infof("Batch %d acknowledged", batchNumber)
			return nil
		case <-ctx.Done():
			return i18n.NewError(ctx, i18n.MsgWebSocketInterruptedReceive)
		}
	}
}
