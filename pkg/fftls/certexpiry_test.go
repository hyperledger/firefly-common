// Copyright © 2026 Kaleido, Inc.
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
	"crypto/x509"
	_ "embed"
	"encoding/pem"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/hyperledger/firefly-common/pkg/config"
	"github.com/hyperledger/firefly-common/pkg/metric"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The certificates below are static self-signed fixtures generated once with openssl, e.g.:
//
//	openssl req -x509 -newkey rsa:2048 -keyout leaf-key.pem -out leaf-cert.pem \
//	  -days 3650 -nodes -subj "/CN=leaf/O=FireFly" -addext "subjectAltName=IP:127.0.0.1"
//
// ca-bundle.pem is the concatenation of ca-a-cert.pem and ca-b-cert.pem.
var (
	//go:embed testdata/ca-a-cert.pem
	caACertPEM []byte
	//go:embed testdata/ca-bundle.pem
	caBundlePEM []byte
	//go:embed testdata/leaf-cert.pem
	leafCertPEM []byte
	//go:embed testdata/leaf-key.pem
	leafKeyPEM []byte
)

func mustParseCert(t *testing.T, pemBytes []byte) *x509.Certificate {
	block, _ := pem.Decode(pemBytes)
	require.NotNil(t, block)
	cert, err := x509.ParseCertificate(block.Bytes)
	require.NoError(t, err)
	return cert
}

func writeTemp(t *testing.T, dir, name string, content []byte) string {
	p := filepath.Join(dir, name)
	require.NoError(t, os.WriteFile(p, content, 0600))
	return p
}

// findCertExpiry gathers from the registry and returns the value of the ff_tls_certificate_expiry
// gauge for the series matching the given "type" label and whose "subject" label contains the supplied
// substring.
func findCertExpiry(t *testing.T, mr metric.MetricsRegistry, certType, subjectSubstr string) (float64, bool) {
	mfs, err := mr.GetGatherer().Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() != "ff_tls_certificate_expiry" {
			continue
		}
		for _, m := range mf.GetMetric() {
			var typeMatch, subjectMatch bool
			for _, lp := range m.GetLabel() {
				if lp.GetName() == "type" && lp.GetValue() == certType {
					typeMatch = true
				}
				if lp.GetName() == "subject" && strings.Contains(lp.GetValue(), subjectSubstr) {
					subjectMatch = true
				}
			}
			if typeMatch && subjectMatch {
				return m.GetGauge().GetValue(), true
			}
		}
	}
	return 0, false
}

// enableTestCertMetrics resets the package-level manager and binds it to a fresh registry for the test.
func enableTestCertMetrics(t *testing.T, component string) metric.MetricsRegistry {
	certMetricsManager = nil
	t.Cleanup(func() { certMetricsManager = nil })
	mr := metric.NewPrometheusMetricsRegistry(component)
	require.NoError(t, EnableCertificateMetrics(context.Background(), mr))
	return mr
}

func TestEnableCertificateMetricsRecordsCAAndClientExpiry(t *testing.T) {
	mr := enableTestCertMetrics(t, "test_fftls_client")

	dir := t.TempDir()
	caBundleFile := writeTemp(t, dir, "ca-bundle.pem", caBundlePEM)
	leafCertFile := writeTemp(t, dir, "leaf-cert.pem", leafCertPEM)
	leafKeyFile := writeTemp(t, dir, "leaf-key.pem", leafKeyPEM)

	caA := mustParseCert(t, caACertPEM)
	caB := mustParseCert(t, secondCertInBundle(t, caBundlePEM))
	leaf := mustParseCert(t, leafCertPEM)

	conf := config.RootSection("fftls_metrics_client")
	InitTLSConfig(conf)
	conf.Set(HTTPConfTLSEnabled, true)
	conf.Set(HTTPConfTLSCAFile, caBundleFile)
	conf.Set(HTTPConfTLSCertFile, leafCertFile)
	conf.Set(HTTPConfTLSKeyFile, leafKeyFile)

	_, err := ConstructTLSConfig(context.Background(), conf, ClientType)
	require.NoError(t, err)

	// Each certificate in the CA bundle gets its own series under the type="ca" label
	caAVal, ok := findCertExpiry(t, mr, "ca", "ca-a")
	require.True(t, ok, "ca-a expiry gauge not found")
	assert.Equal(t, float64(caA.NotAfter.Unix()), caAVal)

	caBVal, ok := findCertExpiry(t, mr, "ca", "ca-b")
	require.True(t, ok, "ca-b expiry gauge not found")
	assert.Equal(t, float64(caB.NotAfter.Unix()), caBVal)

	// A client TLS config records the leaf under type="client"
	clientVal, ok := findCertExpiry(t, mr, "client", "leaf")
	require.True(t, ok, "client expiry gauge not found")
	assert.Equal(t, float64(leaf.NotAfter.Unix()), clientVal)

	// ... and nothing should have been recorded under type="server"
	_, ok = findCertExpiry(t, mr, "server", "leaf")
	assert.False(t, ok, "server gauge should not be set for a client config")
}

func TestEnableCertificateMetricsRecordsServerExpiryInline(t *testing.T) {
	mr := enableTestCertMetrics(t, "test_fftls_server")

	caA := mustParseCert(t, caACertPEM)
	leaf := mustParseCert(t, leafCertPEM)

	// Inline (non-file) PEM material is recorded the same way
	conf := &Config{
		Enabled: true,
		CA:      string(caACertPEM),
		Cert:    string(leafCertPEM),
		Key:     string(leafKeyPEM),
	}
	_, err := NewTLSConfig(context.Background(), conf, ServerType)
	require.NoError(t, err)

	caAVal, ok := findCertExpiry(t, mr, "ca", "ca-a")
	require.True(t, ok, "ca-a expiry gauge not found")
	assert.Equal(t, float64(caA.NotAfter.Unix()), caAVal)

	// A server TLS config records the leaf under type="server"
	serverVal, ok := findCertExpiry(t, mr, "server", "leaf")
	require.True(t, ok, "server expiry gauge not found")
	assert.Equal(t, float64(leaf.NotAfter.Unix()), serverVal)

	_, ok = findCertExpiry(t, mr, "client", "leaf")
	assert.False(t, ok, "client gauge should not be set for a server config")
}

func TestEnableCertificateMetricsIdempotent(t *testing.T) {
	certMetricsManager = nil
	t.Cleanup(func() { certMetricsManager = nil })
	ctx := context.Background()
	mr := metric.NewPrometheusMetricsRegistry("test_fftls_idem")
	require.NoError(t, EnableCertificateMetrics(ctx, mr))
	// Second call is a no-op and must not error (would otherwise fail re-registering the subsystem)
	require.NoError(t, EnableCertificateMetrics(ctx, mr))
}

func TestEnableCertificateMetricsError(t *testing.T) {
	certMetricsManager = nil
	t.Cleanup(func() { certMetricsManager = nil })
	ctx := context.Background()
	mr := metric.NewPrometheusMetricsRegistry("test_fftls_err")
	// Claim the "tls" subsystem before fftls can, forcing a registration error
	_, _ = mr.NewMetricsManagerForSubsystem(ctx, CertMetricsSubsystem)
	err := EnableCertificateMetrics(ctx, mr)
	assert.Error(t, err)
}

func TestNewTLSConfigNoMetricsManagerNoPanic(t *testing.T) {
	// With metrics disabled (the default), loading certs must not panic
	certMetricsManager = nil
	dir := t.TempDir()
	caFile := writeTemp(t, dir, "ca-a-cert.pem", caACertPEM)

	conf := config.RootSection("fftls_no_metrics")
	InitTLSConfig(conf)
	conf.Set(HTTPConfTLSEnabled, true)
	conf.Set(HTTPConfTLSCAFile, caFile)

	tlsConfig, err := ConstructTLSConfig(context.Background(), conf, ClientType)
	require.NoError(t, err)
	assert.NotNil(t, tlsConfig)
}

// secondCertInBundle returns the PEM bytes of the second certificate in a bundle, for parsing.
func secondCertInBundle(t *testing.T, bundlePEM []byte) []byte {
	_, rest := pem.Decode(bundlePEM)
	require.NotEmpty(t, rest)
	return rest
}
