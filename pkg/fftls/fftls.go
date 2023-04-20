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

package fftls

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"os"

	"github.com/hyperledger/firefly-common/pkg/config"
	"github.com/hyperledger/firefly-common/pkg/i18n"
	"github.com/hyperledger/firefly-common/pkg/log"
)

const (
	ServerType = "server"
	ClientType = "client"
)

func ConstructTLSConfig(ctx context.Context, conf config.Section, tlsType string) (*tls.Config, error) {
	tlsConfig := &tls.Config{
		MinVersion: tls.VersionTLS12,
		VerifyPeerCertificate: func(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) error {
			cert := verifiedChains[0][0]
			log.L(ctx).Debugf("Client certificate provided Subject=%s Issuer=%s Expiry=%s", cert.Subject, cert.Issuer, cert.NotAfter)
			return nil
		},
	}

	if !conf.GetBool(HTTPConfTLSEnabled) {
		return tlsConfig, nil
	}

	var err error
	// Support custom CA file
	var rootCAs *x509.CertPool
	caFile := conf.GetString(HTTPConfTLSCAFile)
	if caFile != "" {
		rootCAs = x509.NewCertPool()
		var caBytes []byte
		caBytes, err = os.ReadFile(caFile)
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

	tlsConfig.RootCAs = rootCAs

	// For mTLS we need both the cert and key
	certFile := conf.GetString(HTTPConfTLSCertFile)
	keyFile := conf.GetString(HTTPConfTLSKeyFile)
	if certFile != "" && keyFile != "" {
		// Read the key pair to create certificate
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, i18n.WrapError(ctx, err, i18n.MsgInvalidKeyPairFiles)
		}

		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	if tlsType == ServerType {

		// Support client auth
		tlsConfig.ClientAuth = tls.NoClientCert
		if conf.GetBool(HTTPConfTLSClientAuth) {
			tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert

			// Used to verify a client certificate by the policy in ClientAuth.
			tlsConfig.ClientCAs = rootCAs
		}
	}

	return tlsConfig, nil

}
