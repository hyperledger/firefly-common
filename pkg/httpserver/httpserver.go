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

package httpserver

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"time"

	"github.com/gorilla/mux"
	"github.com/hyperledger/firefly-common/pkg/config"
	"github.com/hyperledger/firefly-common/pkg/fftypes"
	"github.com/hyperledger/firefly-common/pkg/i18n"
	"github.com/hyperledger/firefly-common/pkg/log"
)

type HTTPServer interface {
	ServeHTTP(ctx context.Context)
	Addr() net.Addr
}

type GoHTTPServer interface {
	Close() error
	Serve(l net.Listener) error
	ServeTLS(l net.Listener, certFile, keyFile string) error
	Shutdown(ctx context.Context) error
}

type httpServer struct {
	name            string
	s               GoHTTPServer
	l               net.Listener
	conf            config.Section
	corsConf        config.Section
	onClose         chan error
	tlsEnabled      bool
	tlsCertFile     string
	tlsKeyFile      string
	shutdownTimeout time.Duration
}

func NewHTTPServer(ctx context.Context, name string, r *mux.Router, onClose chan error, conf config.Section, corsConf config.Section) (is HTTPServer, err error) {
	hs := &httpServer{
		name:            name,
		onClose:         onClose,
		conf:            conf,
		corsConf:        corsConf,
		tlsEnabled:      conf.GetBool(HTTPConfTLSEnabled),
		tlsCertFile:     conf.GetString(HTTPConfTLSCertFile),
		tlsKeyFile:      conf.GetString(HTTPConfTLSKeyFile),
		shutdownTimeout: conf.GetDuration(HTTPConfShutdownTimeout),
	}
	hs.l, err = hs.createListener(ctx)
	if err == nil {
		hs.s, err = hs.createServer(ctx, r)
	}
	return hs, err
}

func (hs *httpServer) Addr() net.Addr {
	return hs.l.Addr()
}

func (hs *httpServer) createListener(ctx context.Context) (net.Listener, error) {
	listenAddr := fmt.Sprintf("%s:%d", hs.conf.GetString(HTTPConfAddress), hs.conf.GetUint(HTTPConfPort))
	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return nil, i18n.WrapError(ctx, err, i18n.MsgAPIServerStartFailed, listenAddr)
	}
	log.L(ctx).Infof("%s listening on HTTP %s", hs.name, listener.Addr())
	return listener, err
}

func (hs *httpServer) createServer(ctx context.Context, r *mux.Router) (srv *http.Server, err error) {

	// Support client auth
	clientAuth := tls.NoClientCert
	if hs.conf.GetBool(HTTPConfTLSClientAuth) {
		clientAuth = tls.RequireAndVerifyClientCert
	}

	// Support custom CA file
	var rootCAs *x509.CertPool
	caFile := hs.conf.GetString(HTTPConfTLSCAFile)
	if caFile != "" {
		rootCAs = x509.NewCertPool()
		var caBytes []byte
		caBytes, err = ioutil.ReadFile(caFile)
		if err == nil {
			ok := rootCAs.AppendCertsFromPEM(caBytes)
			if !ok {
				err = i18n.NewError(ctx, i18n.MsgInvalidCAFile)
			}
		}
	} else {
		rootCAs, err = x509.SystemCertPool()
	}

	if err != nil {
		return nil, i18n.WrapError(ctx, err, i18n.MsgTLSConfigFailed)
	}

	srv = &http.Server{
		Handler:      wrapCorsIfEnabled(ctx, hs.corsConf, r),
		WriteTimeout: hs.conf.GetDuration(HTTPConfWriteTimeout),
		ReadTimeout:  hs.conf.GetDuration(HTTPConfReadTimeout),
		TLSConfig: &tls.Config{
			MinVersion: tls.VersionTLS12,
			ClientAuth: clientAuth,
			ClientCAs:  rootCAs,
			RootCAs:    rootCAs,
			VerifyPeerCertificate: func(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) error {
				cert := verifiedChains[0][0]
				log.L(ctx).Debugf("Client certificate provided Subject=%s Issuer=%s Expiry=%s", cert.Subject, cert.Issuer, cert.NotAfter)
				return nil
			},
		},
		ConnContext: func(newCtx context.Context, c net.Conn) context.Context {
			l := log.L(ctx).WithField("req", fftypes.ShortID())
			newCtx = log.WithLogger(newCtx, l)
			l.Debugf("New HTTP connection: remote=%s local=%s", c.RemoteAddr().String(), c.LocalAddr().String())
			return newCtx
		},
	}
	return srv, nil
}

func (hs *httpServer) ServeHTTP(ctx context.Context) {
	serverEnded := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			log.L(ctx).Infof("API server context canceled - shutting down")
			shutdownContext, cancel := context.WithTimeout(context.Background(), hs.shutdownTimeout)
			defer cancel()
			if err := hs.s.Shutdown(shutdownContext); err != nil {
				hs.onClose <- err
				return
			}
		case <-serverEnded:
			return
		}
	}()

	var err error
	if hs.tlsEnabled {
		err = hs.s.ServeTLS(hs.l, hs.tlsCertFile, hs.tlsKeyFile)
	} else {
		err = hs.s.Serve(hs.l)
	}
	if err == http.ErrServerClosed {
		err = nil
	}
	close(serverEnded)
	log.L(ctx).Infof("API server complete")

	hs.onClose <- err
}
