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

package wsclient

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/hyperledger/firefly-common/pkg/ffresty"
	"github.com/hyperledger/firefly-common/pkg/fftypes"
	"github.com/hyperledger/firefly-common/pkg/i18n"
	"github.com/hyperledger/firefly-common/pkg/log"
	"github.com/hyperledger/firefly-common/pkg/retry"
	"golang.org/x/time/rate"
)

type WSConfig struct {
	HTTPURL                   string             `json:"httpUrl,omitempty"`
	WebSocketURL              string             `json:"wsUrl,omitempty"`
	WSKeyPath                 string             `json:"wsKeyPath,omitempty"`
	ReadBufferSize            int                `json:"readBufferSize,omitempty"`
	WriteBufferSize           int                `json:"writeBufferSize,omitempty"`
	InitialDelay              time.Duration      `json:"initialDelay,omitempty"`
	MaximumDelay              time.Duration      `json:"maximumDelay,omitempty"`
	InitialConnectAttempts    int                `json:"initialConnectAttempts,omitempty"`
	DisableReconnect          bool               `json:"disableReconnect"`
	AuthUsername              string             `json:"authUsername,omitempty"`
	AuthPassword              string             `json:"authPassword,omitempty"`
	ThrottleRequestsPerSecond int                `json:"requestsPerSecond,omitempty"`
	ThrottleBurst             int                `json:"burst,omitempty"`
	HTTPHeaders               fftypes.JSONObject `json:"headers,omitempty"`
	HeartbeatInterval         time.Duration      `json:"heartbeatInterval,omitempty"`
	TLSClientConfig           *tls.Config        `json:"tlsClientConfig,omitempty"`
	ConnectionTimeout         time.Duration      `json:"connectionTimeout,omitempty"`
	// This one cannot be set in JSON - must be configured on the code interface
	ReceiveExt bool
}

// WSPayload allows API consumers of this package to stream data, and inspect the message
// type, rather than just being passed the bytes directly.
type WSPayload struct {
	MessageType int
	Reader      io.Reader
	processed   chan struct{}
}

func NewWSPayload(mt int, r io.Reader) *WSPayload {
	return &WSPayload{
		MessageType: mt,
		Reader:      r,
		processed:   make(chan struct{}),
	}
}

// Must call done on each payload, before being delivered the next
func (wsp *WSPayload) Processed() {
	close(wsp.processed)
}

type WSClient interface {
	Connect() error
	Receive() <-chan []byte
	ReceiveExt() <-chan *WSPayload
	URL() string
	SetURL(url string)
	SetHeader(header, value string)
	Send(ctx context.Context, message []byte) error
	Close()
}

type wsClient struct {
	ctx                  context.Context
	headers              http.Header
	url                  string
	initialRetryAttempts int
	wsdialer             *websocket.Dialer
	wsconn               *websocket.Conn
	retry                retry.Retry
	closed               bool
	useReceiveExt        bool
	receive              chan []byte
	receiveExt           chan *WSPayload
	send                 chan []byte
	sendDone             chan []byte
	closing              chan struct{}
	beforeConnect        WSPreConnectHandler
	afterConnect         WSPostConnectHandler
	disableReconnect     bool
	heartbeatInterval    time.Duration
	heartbeatMux         sync.Mutex
	activePingSent       *time.Time
	lastPingCompleted    time.Time
	rateLimiter          *rate.Limiter
}

// WSPreConnectHandler will be called before every connect/reconnect. Any error returned will prevent the websocket from connecting.
type WSPreConnectHandler func(ctx context.Context, w WSClient) error

// WSPostConnectHandler will be called after every connect/reconnect. Can send data over ws, but must not block listening for data on the ws.
type WSPostConnectHandler func(ctx context.Context, w WSClient) error

func New(ctx context.Context, config *WSConfig, beforeConnect WSPreConnectHandler, afterConnect WSPostConnectHandler) (WSClient, error) {
	l := log.L(ctx)
	wsURL, err := buildWSUrl(ctx, config)
	if err != nil {
		return nil, err
	}

	w := &wsClient{
		ctx: ctx,
		url: wsURL,
		wsdialer: &websocket.Dialer{
			ReadBufferSize:   config.ReadBufferSize,
			WriteBufferSize:  config.WriteBufferSize,
			TLSClientConfig:  config.TLSClientConfig,
			HandshakeTimeout: config.ConnectionTimeout,
		},
		retry: retry.Retry{
			InitialDelay: config.InitialDelay,
			MaximumDelay: config.MaximumDelay,
		},
		initialRetryAttempts: config.InitialConnectAttempts,
		headers:              make(http.Header),
		send:                 make(chan []byte),
		closing:              make(chan struct{}),
		beforeConnect:        beforeConnect,
		afterConnect:         afterConnect,
		heartbeatInterval:    config.HeartbeatInterval,
		useReceiveExt:        config.ReceiveExt,
		disableReconnect:     config.DisableReconnect,
		rateLimiter:          ffresty.GetRateLimiter(config.ThrottleRequestsPerSecond, config.ThrottleBurst),
	}
	if w.useReceiveExt {
		w.receiveExt = make(chan *WSPayload)
	} else {
		w.receive = make(chan []byte)
	}
	for k, v := range config.HTTPHeaders {
		if vs, ok := v.(string); ok {
			w.headers.Set(k, vs)
		}
	}
	authUsername := config.AuthUsername
	authPassword := config.AuthPassword
	if authUsername != "" && authPassword != "" {
		w.headers.Set("Authorization", fmt.Sprintf("Basic %s", base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%s:%s", authUsername, authPassword)))))
	}

	go func() {
		select {
		case <-ctx.Done():
			l.Tracef("WS %s closing due to canceled context", w.url)
			w.Close()
		case <-w.closing:
			l.Tracef("WS %s closing", w.url)
		}
	}()

	return w, nil
}

func (w *wsClient) Connect() error {

	if err := w.connect(true); err != nil {
		return err
	}

	go w.receiveReconnectLoop()

	return nil
}

func (w *wsClient) Close() {
	if !w.closed {
		w.closed = true
		close(w.closing)
		c := w.wsconn
		if c != nil {
			_ = c.Close()
		}
	}
}

func (w *wsClient) Receive() <-chan []byte {
	return w.receive
}

// Must set ReceiveExt on the WSConfig to use this
func (w *wsClient) ReceiveExt() <-chan *WSPayload {
	return w.receiveExt
}

func (w *wsClient) URL() string {
	return w.url
}

func (w *wsClient) SetURL(url string) {
	w.url = url
}

func (w *wsClient) SetHeader(header, value string) {
	w.headers.Set(header, value)
}

func (w *wsClient) Send(ctx context.Context, message []byte) error {
	if w.rateLimiter != nil {
		// Wait for permission to proceed with the request
		err := w.rateLimiter.Wait(ctx)
		if err != nil {
			return err
		}
	}
	// Send
	select {
	case w.send <- message:
		return nil
	case <-ctx.Done():
		return i18n.NewError(ctx, i18n.MsgWSSendTimedOut)
	case <-w.closing:
		return i18n.NewError(ctx, i18n.MsgWSClosing)
	}
}

func (w *wsClient) heartbeatTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if w.heartbeatInterval > 0 {
		w.heartbeatMux.Lock()
		baseTime := w.lastPingCompleted
		if w.activePingSent != nil {
			// We're waiting for a pong
			baseTime = *w.activePingSent
		}
		waitTime := w.heartbeatInterval - time.Since(baseTime) // if negative, will pop immediately
		w.heartbeatMux.Unlock()
		return context.WithTimeout(ctx, waitTime)
	}
	return context.WithCancel(ctx)
}

func buildWSUrl(ctx context.Context, config *WSConfig) (string, error) {
	if config.WebSocketURL != "" {
		u, err := url.Parse(config.WebSocketURL)
		if err != nil || !strings.HasPrefix(u.Scheme, "ws") {
			return "", i18n.WrapError(ctx, err, i18n.MsgInvalidWebSocketURL, config.WebSocketURL)
		}
		return u.String(), nil
	}

	u, err := url.Parse(config.HTTPURL)
	if err != nil {
		return "", i18n.WrapError(ctx, err, i18n.MsgInvalidURL, config.HTTPURL)
	}
	if config.WSKeyPath != "" {
		u.Path = config.WSKeyPath
	}
	if u.Scheme == "http" {
		u.Scheme = "ws"
	}
	if u.Scheme == "https" {
		u.Scheme = "wss"
	}
	if config.AuthUsername == "" && config.AuthPassword == "" && u.User != nil {
		config.AuthUsername = u.User.Username()
		config.AuthPassword, _ = u.User.Password()
	}
	u.User = nil
	return u.String(), nil
}

func (w *wsClient) connect(initial bool) error {
	l := log.L(w.ctx)
	return w.retry.DoCustomLog(w.ctx, func(attempt int) (retry bool, err error) {
		if w.closed {
			return false, i18n.NewError(w.ctx, i18n.MsgWSClosing)
		}

		retry = !initial || attempt < w.initialRetryAttempts
		if w.beforeConnect != nil {
			if err = w.beforeConnect(w.ctx, w); err != nil {
				l.Warnf("WS %s connect attempt %d failed in beforeConnect", w.url, attempt)
				return retry, err
			}
		}

		var res *http.Response
		w.wsconn, res, err = w.wsdialer.Dial(w.url, w.headers)
		if err != nil {
			var b []byte
			var status = -1
			if res != nil {
				b, _ = io.ReadAll(res.Body)
				res.Body.Close()
				status = res.StatusCode
			}
			l.Warnf("WS %s connect attempt %d failed [%d]: %s", w.url, attempt, status, string(b))
			return retry, i18n.WrapError(w.ctx, err, i18n.MsgWSConnectFailed)
		}

		w.pongReceivedOrReset(false)
		w.wsconn.SetPongHandler(w.pongHandler)
		l.Infof("WS %s connected", w.url)
		return false, nil
	})
}

func (w *wsClient) readLoop() {
	l := log.L(w.ctx)
	for {
		mt, message, err := w.wsconn.ReadMessage()
		if err != nil {
			// We treat this as informational, as it's normal for the client to disconnect here
			l.Infof("WS %s closed: %s", w.url, err)
			return
		}

		// Pass the message to the consumer
		l.Tracef("WS %s read (mt=%d): %s", w.url, mt, message)
		select {
		case <-w.sendDone:
			l.Debugf("WS %s closing reader after send error", w.url)
			return
		case w.receive <- message:
		}
	}
}

func (w *wsClient) readLoopExt() {
	l := log.L(w.ctx)
	for {
		// We set a deadline for twice the heartbeat interval - note we bump this on pong
		if w.heartbeatInterval > 0 {
			_ = w.wsconn.SetReadDeadline(time.Now().Add(2 * w.heartbeatInterval))
		}

		mt, r, err := w.wsconn.NextReader()
		if err != nil {
			// We treat this as informational, as it's normal for the client to disconnect here
			l.Infof("WS %s closed: %s", w.url, err)
			return
		}

		// Pass the message to the consumer
		l.Tracef("WS %s read (mt=%d)", w.url, mt)
		payload := NewWSPayload(mt, r)
		select {
		case <-w.sendDone:
			l.Debugf("WS %s closing reader after send error (waiting for data)", w.url)
			return
		case w.receiveExt <- payload:
		}
		select {
		case <-payload.processed:
			// It's the callers responsibility to ensure they call done on this before we can get the next payload
		case <-w.sendDone:
			l.Debugf("WS %s closing reader after send error (waiting for processing of data by client)", w.url)
			return
		}
	}
}

func (w *wsClient) pongHandler(_ string) error {
	w.pongReceivedOrReset(true)
	return nil
}

func (w *wsClient) pongReceivedOrReset(isPong bool) {
	w.heartbeatMux.Lock()
	defer w.heartbeatMux.Unlock()

	if isPong && w.activePingSent != nil {
		log.L(w.ctx).Debugf("WS %s heartbeat completed (pong) after %.2fms", w.url, float64(time.Since(*w.activePingSent))/float64(time.Millisecond))
	}
	w.lastPingCompleted = time.Now() // in new connection case we still want to consider now the time we completed the ping
	w.activePingSent = nil

	// We set a deadline for twice the heartbeat interval
	if w.heartbeatInterval > 0 {
		_ = w.wsconn.SetReadDeadline(time.Now().Add(2 * w.heartbeatInterval))
	}

}

func (w *wsClient) heartbeatCheck() error {
	w.heartbeatMux.Lock()
	defer w.heartbeatMux.Unlock()

	if w.activePingSent != nil {
		return i18n.NewError(w.ctx, i18n.MsgWSHeartbeatTimeout, float64(time.Since(*w.activePingSent))/float64(time.Millisecond))
	}
	log.L(w.ctx).Debugf("WS %s heartbeat timer popped (ping) after %.2fms", w.url, float64(time.Since(w.lastPingCompleted))/float64(time.Millisecond))
	now := time.Now()
	w.activePingSent = &now
	return nil
}

func (w *wsClient) sendLoop(receiverDone chan struct{}) {
	l := log.L(w.ctx)
	defer close(w.sendDone)

	disconnecting := false
	for !disconnecting {
		timeoutContext, timeoutCancel := w.heartbeatTimeout(w.ctx)

		select {
		case message := <-w.send:
			l.Tracef("WS sending: %s", message)
			if err := w.wsconn.WriteMessage(websocket.TextMessage, message); err != nil {
				l.Errorf("WS %s send failed: %s", w.url, err)
				disconnecting = true
			}
		case <-timeoutContext.Done():
			wsconn := w.wsconn
			if err := w.heartbeatCheck(); err != nil {
				l.Errorf("WS %s closing: %s", w.url, err)
				disconnecting = true
			} else if wsconn != nil {
				if err := wsconn.WriteMessage(websocket.PingMessage, []byte{}); err != nil {
					l.Errorf("WS %s heartbeat send failed: %s", w.url, err)
					disconnecting = true
				}
			}
		case <-receiverDone:
			l.Debugf("WS %s send loop exiting", w.url)
			disconnecting = true
		}

		timeoutCancel()
	}
}

func (w *wsClient) receiveReconnectLoop() {
	l := log.L(w.ctx)
	if w.useReceiveExt {
		defer close(w.receiveExt)
	} else {
		defer close(w.receive)
	}
	for !w.closed {
		// Start the sender, letting it close without blocking sending a notification on the sendDone
		w.sendDone = make(chan []byte, 1)
		receiverDone := make(chan struct{})
		go w.sendLoop(receiverDone)

		// Call the reconnect processor
		var err error
		if w.afterConnect != nil {
			err = w.afterConnect(w.ctx, w)
		}

		if err == nil {
			// Synchronously invoke the reader, as it's important we react immediately to any error there.
			if w.useReceiveExt {
				w.readLoopExt()
			} else {
				w.readLoop()
			}
			close(receiverDone)
			<-w.sendDone

			// Ensure the connection is closed after the sender and receivers exit
			err = w.wsconn.Close()
			if err != nil {
				l.Debugf("WS %s close failed: %s", w.url, err)
			}
			w.sendDone = nil
			w.wsconn = nil
		}

		if w.disableReconnect {
			return
		}

		// Go into reconnect
		if !w.closed {
			err = w.connect(false)
			if err != nil {
				l.Debugf("WS %s exiting: %s", w.url, err)
				return
			}
		}
	}
}
