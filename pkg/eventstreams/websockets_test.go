// Copyright © 2022 Kaleido, Inc.
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
	"fmt"
	"testing"

	"github.com/hyperledger/firefly-common/mocks/wsservermocks"
	"github.com/hyperledger/firefly-common/pkg/ffapi"
	"github.com/hyperledger/firefly-common/pkg/fftypes"
	"github.com/hyperledger/firefly-common/pkg/wsserver"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func mockWSChannels(wsc *wsservermocks.WebSocketChannels) (chan interface{}, chan interface{}, chan *wsserver.WebSocketCommandMessageOrError) {
	senderChannel := make(chan interface{}, 1)
	broadcastChannel := make(chan interface{}, 1)
	receiverChannel := make(chan *wsserver.WebSocketCommandMessageOrError, 1)
	wsc.On("GetChannels", "ut_stream").Return((chan<- interface{})(senderChannel), (chan<- interface{})(broadcastChannel), (<-chan *wsserver.WebSocketCommandMessageOrError)(receiverChannel)).Maybe()
	return senderChannel, broadcastChannel, receiverChannel
}

func newTestWebSocketsFactory(t *testing.T) (context.Context, *esManager[*GenericEventStream, testData], *wsservermocks.WebSocketChannels, *webSocketDispatcherFactory[*GenericEventStream, testData]) {
	ctx, mgr, _, done := newMockESManager(t, func(mdb *mockPersistence) {
		mdb.eventStreams.On("GetMany", mock.Anything, mock.Anything).Return([]*GenericEventStream{}, &ffapi.FilterResult{}, nil)
	})
	done()

	mws := wsservermocks.NewWebSocketChannels(t)
	mgr.wsChannels = mws

	return ctx, mgr, mws, &webSocketDispatcherFactory[*GenericEventStream, testData]{esm: mgr}
}

func TestWSAttemptIgnoreWrongAcks(t *testing.T) {

	ctx, mgr, mws, whf := newTestWebSocketsFactory(t)
	_, _, rc := mockWSChannels(mws)

	go func() {
		rc <- &wsserver.WebSocketCommandMessageOrError{Msg: &wsserver.WebSocketCommandMessage{
			BatchNumber: 12345,
		}}
		rc <- &wsserver.WebSocketCommandMessageOrError{Msg: &wsserver.WebSocketCommandMessage{
			BatchNumber: 23456,
		}}
	}()

	dmw := DistributionModeBroadcast
	spec := &GenericEventStream{
		EventStreamSpecFields: EventStreamSpecFields{
			Name: ptrTo("ut_stream"),
		},
		WebSocket: &WebSocketConfig{
			DistributionMode: &dmw,
		},
	}
	wsa := whf.NewDispatcher(ctx, &mgr.config, spec).(*webSocketAction[testData])

	err := wsa.AttemptDispatch(context.Background(), 0, &EventBatch[testData]{
		StreamID:    fftypes.NewUUID().String(),
		BatchNumber: 1,
		Events: []*Event[testData]{
			{Data: &testData{Field1: 12345}},
		},
	})
	assert.NoError(t, err)

	err = wsa.waitForAck(context.Background(), rc, 23456)
	assert.NoError(t, err)
}

func TestWSattemptDispatchExitPushingEvent(t *testing.T) {

	ctx, mgr, mws, whf := newTestWebSocketsFactory(t)
	_, bc, _ := mockWSChannels(mws)
	bc <- []*fftypes.JSONAny{} // block the broadcast channel

	dmw := DistributionModeBroadcast
	spec := &GenericEventStream{
		EventStreamSpecFields: EventStreamSpecFields{
			Name: ptrTo("ut_stream"),
		},
		WebSocket: &WebSocketConfig{
			DistributionMode: &dmw,
		},
	}
	wsa := whf.NewDispatcher(ctx, &mgr.config, spec).(*webSocketAction[testData])

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := wsa.AttemptDispatch(ctx, 0, &EventBatch[testData]{
		StreamID:    fftypes.NewUUID().String(),
		BatchNumber: 1,
		Events: []*Event[testData]{
			{Data: &testData{Field1: 12345}},
		},
	})
	assert.Regexp(t, "FF00225", err)

}

func TestWSattemptDispatchExitReceivingReply(t *testing.T) {

	ctx, mgr, mws, whf := newTestWebSocketsFactory(t)
	_, _, rc := mockWSChannels(mws)

	dmw := DistributionModeBroadcast
	spec := &GenericEventStream{
		EventStreamSpecFields: EventStreamSpecFields{
			Name: ptrTo("ut_stream"),
		},
		WebSocket: &WebSocketConfig{
			DistributionMode: &dmw,
		},
	}
	wsa := whf.NewDispatcher(ctx, &mgr.config, spec).(*webSocketAction[testData])

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := wsa.waitForAck(ctx, rc, -1)
	assert.Regexp(t, "FF00226", err)

}

func TestWSattemptDispatchNackFromClient(t *testing.T) {

	ctx, mgr, mws, whf := newTestWebSocketsFactory(t)
	_, _, rc := mockWSChannels(mws)
	rc <- &wsserver.WebSocketCommandMessageOrError{
		Err: fmt.Errorf("pop"),
	}

	dmw := DistributionModeBroadcast
	spec := &GenericEventStream{
		EventStreamSpecFields: EventStreamSpecFields{
			Name: ptrTo("ut_stream"),
		},
		WebSocket: &WebSocketConfig{
			DistributionMode: &dmw,
		},
	}
	wsa := whf.NewDispatcher(ctx, &mgr.config, spec).(*webSocketAction[testData])

	err := wsa.waitForAck(context.Background(), rc, -1)
	assert.Regexp(t, "pop", err)

}
