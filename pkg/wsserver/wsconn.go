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

package wsserver

import (
	"context"
	"reflect"
	"strings"
	"sync"

	ws "github.com/gorilla/websocket"
	"github.com/hyperledger/firefly-common/pkg/fftypes"
	"github.com/hyperledger/firefly-common/pkg/i18n"
	"github.com/hyperledger/firefly-common/pkg/log"
)

type webSocketConnection struct {
	ctx       context.Context
	id        string
	server    *webSocketServer
	conn      *ws.Conn
	mux       sync.Mutex
	closed    bool
	streams   map[string]*webSocketStream
	broadcast chan interface{}
	newStream chan bool
	closing   chan struct{}
}

type WebSocketCommandMessageOrError struct {
	Msg *WebSocketCommandMessage
	Err error
}

type WebSocketCommandMessage struct {
	Type        string `json:"type,omitempty"`
	Stream      string `json:"stream,omitempty"` // name of the event stream
	Message     string `json:"message,omitempty"`
	BatchNumber int64  `json:"batchNumber,omitempty"`
}

func newConnection(bgCtx context.Context, server *webSocketServer, conn *ws.Conn) *webSocketConnection {
	id := fftypes.NewUUID().String()
	wsc := &webSocketConnection{
		ctx:       log.WithLogField(bgCtx, "wsc", id),
		id:        id,
		server:    server,
		conn:      conn,
		newStream: make(chan bool),
		streams:   make(map[string]*webSocketStream),
		broadcast: make(chan interface{}),
		closing:   make(chan struct{}),
	}
	go wsc.listen()
	go wsc.sender()
	return wsc
}

func (c *webSocketConnection) close() {
	c.mux.Lock()
	if !c.closed {
		c.closed = true
		c.conn.Close()
		close(c.closing)
	}
	c.mux.Unlock()

	for _, t := range c.streams {
		c.server.cycleStream(c.id, t)
		log.L(c.ctx).Infof("Websocket closed while active on stream '%s'", t.streamName)
	}
	c.server.connectionClosed(c)
	log.L(c.ctx).Infof("Disconnected")
}

func (c *webSocketConnection) sender() {
	defer c.close()
	buildCases := func() []reflect.SelectCase {
		c.mux.Lock()
		defer c.mux.Unlock()
		cases := make([]reflect.SelectCase, len(c.streams)+3)
		i := 0
		for _, t := range c.streams {
			cases[i] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(t.senderChannel)}
			i++
		}
		cases[i] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(c.broadcast)}
		i++
		cases[i] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(c.closing)}
		i++
		cases[i] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(c.newStream)}
		return cases
	}
	cases := buildCases()
	for {
		chosen, value, ok := reflect.Select(cases)
		if !ok {
			log.L(c.ctx).Infof("Closing")
			return
		}

		if chosen == len(cases)-1 {
			// Addition of a new stream
			cases = buildCases()
		} else {
			// Message from one of the existing streams
			_ = c.conn.WriteJSON(value.Interface())
		}
	}
}

func (c *webSocketConnection) startStream(t *webSocketStream) {
	c.mux.Lock()
	c.streams[t.streamName] = t
	c.server.StreamStarted(c, t.streamName)
	c.mux.Unlock()
	select {
	case c.newStream <- true:
	case <-c.closing:
	}
}

func (c *webSocketConnection) listen() {
	defer c.close()
	log.L(c.ctx).Infof("Connected")
	for {
		var msg WebSocketCommandMessage
		err := c.conn.ReadJSON(&msg)
		if err != nil {
			log.L(c.ctx).Errorf("Error: %s", err)
			return
		}
		log.L(c.ctx).Tracef("Received: %+v", msg)

		stream := msg.Stream
		if stream == "" {
			stream = msg.Stream
		}
		t := c.server.getStream(stream)
		switch strings.ToLower(msg.Type) {
		case "start":
			c.startStream(t)
		case "ack":
			if !c.dispatchAckOrError(t, &msg, nil) {
				return
			}
		case "error":
			if !c.dispatchAckOrError(t, &msg, i18n.NewError(c.ctx, i18n.MsgWSErrorFromClient, msg.Message)) {
				return
			}
		default:
			log.L(c.ctx).Errorf("Unexpected message type: %+v", msg)
		}
	}
}

func (c *webSocketConnection) dispatchAckOrError(t *webSocketStream, msg *WebSocketCommandMessage, err error) bool {
	if err != nil {
		log.L(c.ctx).Debugf("Received WebSocket error on stream '%s': %s", t.streamName, err)
	} else {
		log.L(c.ctx).Debugf("Received WebSocket ack for batch %d on stream '%s'", msg.BatchNumber, t.streamName)
	}
	select {
	case t.receiverChannel <- &WebSocketCommandMessageOrError{Msg: msg, Err: err}:
	default:
		log.L(c.ctx).Debugf("Received WebSocket ack for batch %d on stream '%s'. Too many spurious acks - closing connection", msg.BatchNumber, t.streamName)
		// This shouldn't happen, as the channel has a buffer. So this means the client has sent us a number of
		// acks that are not on the right stream (so no event stream is attached).
		// We cannot discard this ack and continue, but we cannot afford to block here either, so we close the websocket
		return false
	}
	return true
}
