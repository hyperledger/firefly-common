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
	"crypto/x509/pkix"
	"os"
	"regexp"
	"strings"

	"github.com/hyperledger/firefly-common/pkg/config"
	"github.com/hyperledger/firefly-common/pkg/i18n"
	"github.com/hyperledger/firefly-common/pkg/log"
)

type TLSType string

const (
	ServerType TLSType = "server"
	ClientType TLSType = "client"
)

func ConstructTLSConfig(ctx context.Context, conf config.Section, tlsType TLSType) (*tls.Config, error) {
	return NewTLSConfig(ctx, GenerateConfig(conf), tlsType)
}

func NewTLSConfig(ctx context.Context, config *Config, tlsType TLSType) (*tls.Config, error) {
	if !config.Enabled {
		return nil, nil
	}

	tlsConfig := &tls.Config{
		MinVersion: tls.VersionTLS12,
		VerifyPeerCertificate: func(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) error {
			if len(verifiedChains) > 0 && len(verifiedChains[0]) > 0 {
				cert := verifiedChains[0][0]
				log.L(ctx).Debugf("Client certificate provided Subject=%s Issuer=%s Expiry=%s", cert.Subject, cert.Issuer, cert.NotAfter)
			} else {
				log.L(ctx).Debugf("Client certificate unverified")
			}
			return nil
		},
	}

	var err error
	// Support custom CA file
	var rootCAs *x509.CertPool
	if config.CAFile != "" {
		rootCAs, err = x509.SystemCertPool()
		if err != nil {
			return nil, err
		}
		var caBytes []byte
		caBytes, err = os.ReadFile(config.CAFile)
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
	if config.CertFile != "" && config.KeyFile != "" {
		// Read the key pair to create certificate
		cert, err := tls.LoadX509KeyPair(config.CertFile, config.KeyFile)
		if err != nil {
			return nil, i18n.WrapError(ctx, err, i18n.MsgInvalidKeyPairFiles)
		}

		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	if tlsType == ServerType {

		// Support client auth
		tlsConfig.ClientAuth = tls.NoClientCert
		if config.ClientAuth {
			tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert

			// Used to verify a client certificate by the policy in ClientAuth.
			tlsConfig.ClientCAs = rootCAs
		}

	}

	if len(config.RequiredDNAttributes) > 0 {
		if tlsConfig.VerifyPeerCertificate, err = buildDNValidator(ctx, config.RequiredDNAttributes); err != nil {
			return nil, err
		}
	}

	tlsConfig.InsecureSkipVerify = config.InsecureSkipHostVerify

	return tlsConfig, nil

}

var SubjectDNKnownAttributes = map[string]func(pkix.Name) []string{
	"C": func(n pkix.Name) []string {
		return n.Country
	},
	"O": func(n pkix.Name) []string {
		return n.Organization
	},
	"OU": func(n pkix.Name) []string {
		return n.OrganizationalUnit
	},
	"CN": func(n pkix.Name) []string {
		if n.CommonName == "" {
			return []string{}
		}
		return []string{n.CommonName}
	},
	"SERIALNUMBER": func(n pkix.Name) []string {
		if n.SerialNumber == "" {
			return []string{}
		}
		return []string{n.SerialNumber}
	},
	"L": func(n pkix.Name) []string {
		return n.Locality
	},
	"ST": func(n pkix.Name) []string {
		return n.Province
	},
	"STREET": func(n pkix.Name) []string {
		return n.StreetAddress
	},
	"POSTALCODE": func(n pkix.Name) []string {
		return n.PostalCode
	},
}

func buildDNValidator(ctx context.Context, requiredDNAttributes map[string]interface{}) (func(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) error, error) {

	validators := make(map[string]*regexp.Regexp)
	for attr, v := range requiredDNAttributes {
		attr = strings.ToUpper(attr)
		if _, knownAttr := SubjectDNKnownAttributes[attr]; !knownAttr {
			return nil, i18n.NewError(ctx, i18n.MsgInvalidTLSDnMatcherAttr, attr)
		}
		validatorString, ok := v.(string)
		if !ok {
			return nil, i18n.NewError(ctx, i18n.MsgInvalidTLSDnMatcherType, attr, v)
		}
		// Ensure full string match with all regexp
		validatorString = "^" + strings.TrimSuffix(strings.TrimPrefix(validatorString, "^"), "$") + "$"
		validator, err := regexp.Compile(validatorString)
		if err != nil {
			return nil, i18n.NewError(ctx, i18n.MsgInvalidTLSDnMatcherRegexp, validatorString, attr, err)
		}
		validators[attr] = validator
	}
	return func(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) error {
		if len(verifiedChains) == 0 {
			log.L(ctx).Errorf("Failed TLS DN check: Nil cert chain")
			return i18n.NewError(ctx, i18n.MsgInvalidTLSDnChain)
		}
		for iChain, chain := range verifiedChains {
			if len(chain) == 0 {
				log.L(ctx).Errorf("Failed TLS DN check: Empty cert chain %d", iChain)
				return i18n.NewError(ctx, i18n.MsgInvalidTLSDnChain)
			}
			// We get a chain of one or more certificates, leaf first.
			// Only check the leaf.
			cert := chain[0]
			log.L(ctx).Debugf("Performing TLS DN check on '%s'", cert.Subject)
			for attr, validator := range validators {
				matched := false
				values := SubjectDNKnownAttributes[attr](cert.Subject) // Note check above makes this safe
				for _, value := range values {
					matched = matched || validator.MatchString(value)
				}
				if !matched {
					log.L(ctx).Errorf("Failed TLS DN check: Does not match %s =~ /%s/", attr, validator.String())
					return i18n.NewError(ctx, i18n.MsgInvalidTLSDnMismatch)
				}
			}
		}
		return nil
	}, nil

}
