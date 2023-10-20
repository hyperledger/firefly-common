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
	"net/http"
	"reflect"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/hyperledger/firefly-common/pkg/i18n"
	"github.com/hyperledger/firefly-common/pkg/log"
)

// WebSocketChannels is provided to allow us to do a blocking send to a namespace that will complete once a client connects on it
// We also provide a channel to listen on for closing of the connection, to allow a select to wake on a blocking send
type WebSocketChannels interface {
	GetChannels(streamName string) (senderChannel chan<- interface{}, broadcastChannel chan<- interface{}, receiverChannel <-chan *WebSocketCommandMessageOrError)
}

// WebSocketServer is the full server interface with the init call
type WebSocketServer interface {
	WebSocketChannels
	Handler(w http.ResponseWriter, r *http.Request)
	Close()
}

type webSocketServer struct {
	ctx               context.Context
	processingTimeout time.Duration
	mux               sync.Mutex
	streams           map[string]*webSocketStream
	streamMap         map[string]map[string]*webSocketConnection
	newStream         chan bool
	replyChannel      chan interface{}
	upgrader          *websocket.Upgrader
	connections       map[string]*webSocketConnection
}

type webSocketStream struct {
	streamName       string
	senderChannel    chan interface{}
	broadcastChannel chan interface{}
	receiverChannel  chan *WebSocketCommandMessageOrError
}

// NewWebSocketServer create a new server with a simplified interface
func NewWebSocketServer(bgCtx context.Context) WebSocketServer {
	s := &webSocketServer{
		ctx:               bgCtx,
		connections:       make(map[string]*webSocketConnection),
		streams:           make(map[string]*webSocketStream),
		streamMap:         make(map[string]map[string]*webSocketConnection),
		newStream:         make(chan bool),
		replyChannel:      make(chan interface{}),
		processingTimeout: 30 * time.Second,
		upgrader: &websocket.Upgrader{
			ReadBufferSize:  1024,
			WriteBufferSize: 1024,
		},
	}
	go s.processBroadcasts()
	return s
}

func (s *webSocketServer) Handler(w http.ResponseWriter, r *http.Request) {
	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.L(s.ctx).Errorf("WebSocket upgrade failed: %s", err)
		return
	}
	s.mux.Lock()
	defer s.mux.Unlock()
	c := newConnection(s.ctx, s, conn)
	s.connections[c.id] = c
}

func (s *webSocketServer) cycleStream(connInfo string, t *webSocketStream) {
	s.mux.Lock()
	defer s.mux.Unlock()

	// When a connection that was listening on a stream closes, we need to wake anyone
	// that was listening for a response
	select {
	case t.receiverChannel <- &WebSocketCommandMessageOrError{Err: i18n.NewError(s.ctx, i18n.MsgWebSocketClosed, connInfo)}:
	default:
	}
}

func (s *webSocketServer) connectionClosed(c *webSocketConnection) {
	s.mux.Lock()
	defer s.mux.Unlock()
	delete(s.connections, c.id)
	for _, stream := range c.streams {
		delete(s.streamMap[stream.streamName], c.id)
	}
}

func (s *webSocketServer) Close() {
	for _, c := range s.connections {
		c.close()
	}
}

func (s *webSocketServer) getStream(stream string) *webSocketStream {
	s.mux.Lock()
	t, exists := s.streams[stream]
	if !exists {
		t = &webSocketStream{
			streamName:       stream,
			senderChannel:    make(chan interface{}),
			broadcastChannel: make(chan interface{}),
			receiverChannel:  make(chan *WebSocketCommandMessageOrError, 10),
		}
		s.streams[stream] = t
		s.streamMap[stream] = make(map[string]*webSocketConnection)
	}
	s.mux.Unlock()
	if !exists {
		// Signal to the broadcaster that a new stream has been added
		s.newStream <- true
	}
	return t
}

func (s *webSocketServer) GetChannels(stream string) (chan<- interface{}, chan<- interface{}, <-chan *WebSocketCommandMessageOrError) {
	t := s.getStream(stream)
	return t.senderChannel, t.broadcastChannel, t.receiverChannel
}

func (s *webSocketServer) StreamStarted(c *webSocketConnection, stream string) {
	// Track that this connection is interested in this stream
	s.streamMap[stream][c.id] = c
}

func (s *webSocketServer) processBroadcasts() {
	var streams []string
	buildCases := func() []reflect.SelectCase {
		// only hold the lock while we're building the list of cases (not while doing the select)
		s.mux.Lock()
		defer s.mux.Unlock()
		streams = make([]string, len(s.streams))
		cases := make([]reflect.SelectCase, len(s.streams)+1)
		i := 0
		for _, t := range s.streams {
			cases[i] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(t.broadcastChannel)}
			streams[i] = t.streamName
			i++
		}
		cases[i] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(s.newStream)}
		return cases
	}
	cases := buildCases()
	for {
		chosen, value, ok := reflect.Select(cases)
		if !ok {
			log.L(s.ctx).Warn("An error occurred broadcasting the message")
			return
		}

		if chosen == len(cases)-1 {
			// Addition of a new stream
			cases = buildCases()
		} else {
			// Message on one of the existing streams
			// Gather all connections interested in this stream and send to them
			s.mux.Lock()
			stream := streams[chosen]
			wsconns := getConnListFromMap(s.streamMap[stream])
			s.mux.Unlock()
			s.broadcastToConnections(wsconns, value.Interface())
		}
	}
}

// getConnListFromMap is a simple helper to snapshot a map into a list, which can be called with a short-lived lock
func getConnListFromMap(tm map[string]*webSocketConnection) []*webSocketConnection {
	wsconns := make([]*webSocketConnection, 0, len(tm))
	for _, c := range tm {
		wsconns = append(wsconns, c)
	}
	return wsconns
}

func (s *webSocketServer) broadcastToConnections(connections []*webSocketConnection, message interface{}) {
	for _, c := range connections {
		select {
		case c.broadcast <- message:
		case <-c.closing:
			log.L(s.ctx).Warnf("Connection %s closed while attempting to deliver reply", c.id)
		}
	}
}
