// Copyright © 2021 Kaleido, Inc.
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

package ffresty

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"log"
	"math/big"
	"net"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/hyperledger/firefly-common/pkg/config"
	"github.com/hyperledger/firefly-common/pkg/ffapi"
	"github.com/hyperledger/firefly-common/pkg/fftls"
	"github.com/hyperledger/firefly-common/pkg/i18n"
	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/assert"
)

const configDir = "../../test/data/config"

var utConf = config.RootSection("http_unit_tests")

func resetConf() {
	config.RootConfigReset()
	InitConfig(utConf)
}

func TestRequestOK(t *testing.T) {

	customClient := &http.Client{}

	resetConf()
	utConf.Set(HTTPConfigURL, "http://localhost:12345")
	utConf.Set(HTTPConfigHeaders, map[string]interface{}{
		"someheader": "headervalue",
	})
	utConf.Set(HTTPConfigAuthUsername, "user")
	utConf.Set(HTTPConfigAuthPassword, "pass")
	utConf.Set(HTTPConfigRetryEnabled, true)
	utConf.Set(HTTPCustomClient, customClient)

	c, err := New(context.Background(), utConf)
	assert.Nil(t, err)
	httpmock.ActivateNonDefault(customClient)
	defer httpmock.DeactivateAndReset()

	httpmock.RegisterResponder("GET", "http://localhost:12345/test",
		func(req *http.Request) (*http.Response, error) {
			assert.Equal(t, "headervalue", req.Header.Get("someheader"))
			assert.Equal(t, "Basic dXNlcjpwYXNz", req.Header.Get("Authorization"))
			return httpmock.NewStringResponder(200, `{"some": "data"}`)(req)
		})

	resp, err := c.R().Get("/test")
	assert.NoError(t, err)
	assert.Equal(t, 200, resp.StatusCode())
	assert.Equal(t, `{"some": "data"}`, resp.String())

	assert.Equal(t, 1, httpmock.GetTotalCallCount())
}

func TestRequestRetry(t *testing.T) {

	ctx := context.Background()

	resetConf()
	utConf.Set(HTTPConfigURL, "http://localhost:12345")
	utConf.Set(HTTPConfigRetryEnabled, true)
	utConf.Set(HTTPConfigRetryInitDelay, 1)

	c, err := New(ctx, utConf)
	assert.Nil(t, err)
	httpmock.ActivateNonDefault(c.GetClient())
	defer httpmock.DeactivateAndReset()

	httpmock.RegisterResponder("GET", "http://localhost:12345/test",
		httpmock.NewStringResponder(500, `{"message": "pop"}`))

	resp, err := c.R().Get("/test")
	assert.NoError(t, err)
	assert.Equal(t, 500, resp.StatusCode())
	assert.Equal(t, 6, httpmock.GetTotalCallCount())

	err = WrapRestErr(ctx, resp, err, i18n.MsgConfigFailed)
	assert.Error(t, err)

}

func TestConfWithProxy(t *testing.T) {

	ctx := context.Background()

	resetConf()
	utConf.Set(HTTPConfigURL, "http://localhost:12345")
	utConf.Set(HTTPConfigProxyURL, "http://myproxy.example.com:12345")
	utConf.Set(HTTPConfigRetryEnabled, false)

	c, err := New(ctx, utConf)
	assert.Nil(t, err)
	assert.True(t, c.IsProxySet())
}

func TestLongResponse(t *testing.T) {

	ctx := context.Background()

	resetConf()
	utConf.Set(HTTPConfigURL, "http://localhost:12345")
	utConf.Set(HTTPConfigRetryEnabled, false)

	c, err := New(ctx, utConf)
	assert.Nil(t, err)
	httpmock.ActivateNonDefault(c.GetClient())
	defer httpmock.DeactivateAndReset()

	resText := strings.Builder{}
	for i := 0; i < 512; i++ {
		resText.WriteByte(byte('a' + (i % 26)))
	}
	httpmock.RegisterResponder("GET", "http://localhost:12345/test",
		httpmock.NewStringResponder(500, resText.String()))

	resp, err := c.R().Get("/test")
	err = WrapRestErr(ctx, resp, err, i18n.MsgConfigFailed)
	assert.Error(t, err)
}

func TestErrResponse(t *testing.T) {

	ctx := context.Background()

	resetConf()
	utConf.Set(HTTPConfigURL, "http://localhost:12345")
	utConf.Set(HTTPConfigRetryEnabled, false)

	c, err := New(ctx, utConf)
	assert.Nil(t, err)
	httpmock.ActivateNonDefault(c.GetClient())
	defer httpmock.DeactivateAndReset()

	resText := strings.Builder{}
	for i := 0; i < 512; i++ {
		resText.WriteByte(byte('a' + (i % 26)))
	}
	httpmock.RegisterResponder("GET", "http://localhost:12345/test",
		httpmock.NewErrorResponder(fmt.Errorf("pop")))

	resp, err := c.R().Get("/test")
	err = WrapRestErr(ctx, resp, err, i18n.MsgConfigFailed)
	assert.Error(t, err)
}

func TestOnAfterResponseNil(t *testing.T) {
	OnAfterResponse(nil, nil)
}

func TestPassthroughHeaders(t *testing.T) {
	passthroughHeaders := http.Header{}
	passthroughHeaders.Set("X-Custom-Header", "custom value")
	ctx := context.WithValue(context.Background(), ffapi.CtxHeadersKey{}, passthroughHeaders)
	ctx = context.WithValue(ctx, ffapi.CtxFFRequestIDKey{}, "customReqID")

	customClient := &http.Client{}

	resetConf()
	utConf.Set(HTTPConfigURL, "http://localhost:12345")
	utConf.Set(HTTPConfigHeaders, map[string]interface{}{
		"someheader": "headervalue",
	})
	utConf.Set(HTTPConfigAuthUsername, "user")
	utConf.Set(HTTPConfigAuthPassword, "pass")
	utConf.Set(HTTPConfigRetryEnabled, true)
	utConf.Set(HTTPCustomClient, customClient)
	utConf.Set(HTTPPassthroughHeadersEnabled, true)

	c, err := New(context.Background(), utConf)
	assert.Nil(t, err)
	httpmock.ActivateNonDefault(customClient)
	defer httpmock.DeactivateAndReset()

	httpmock.RegisterResponder("GET", "http://localhost:12345/test",
		func(req *http.Request) (*http.Response, error) {
			assert.Equal(t, "customReqID", req.Header.Get(ffapi.FFRequestIDHeader))
			assert.Equal(t, "headervalue", req.Header.Get("someheader"))
			assert.Equal(t, "custom value", req.Header.Get("X-Custom-Header"))
			assert.Equal(t, "Basic dXNlcjpwYXNz", req.Header.Get("Authorization"))
			return httpmock.NewStringResponder(200, `{"some": "data"}`)(req)
		})

	resp, err := c.R().SetContext(ctx).Get("/test")
	assert.NoError(t, err)
	assert.Equal(t, 200, resp.StatusCode())
	assert.Equal(t, `{"some": "data"}`, resp.String())

	assert.Equal(t, 1, httpmock.GetTotalCallCount())
}

func TestMissingCAFile(t *testing.T) {
	resetConf()
	utConf.Set(HTTPConfigURL, "https://localhost:12345")
	tlsSection := utConf.SubSection("tls")
	tlsSection.Set(fftls.HTTPConfTLSEnabled, true)
	tlsSection.Set(fftls.HTTPConfTLSCAFile, "non-existent.pem")

	_, err := New(context.Background(), utConf)
	assert.Regexp(t, "FF00153", err)
}

func TestBadCAFile(t *testing.T) {
	resetConf()
	utConf.Set(HTTPConfigURL, "https://localhost:12345")
	tlsSection := utConf.SubSection("tls")
	tlsSection.Set(fftls.HTTPConfTLSEnabled, true)
	tlsSection.Set(fftls.HTTPConfTLSCAFile, configDir+"/firefly.common.yaml")

	_, err := New(context.Background(), utConf)
	assert.Regexp(t, "FF00152", err)
}

func TestBadKeyPair(t *testing.T) {
	resetConf()
	utConf.Set(HTTPConfigURL, "https://localhost:12345")
	tlsSection := utConf.SubSection("tls")
	tlsSection.Set(fftls.HTTPConfTLSEnabled, true)
	tlsSection.Set(fftls.HTTPConfTLSCertFile, configDir+"/firefly.common.yaml")
	tlsSection.Set(fftls.HTTPConfTLSKeyFile, configDir+"/firefly.common.yaml")

	_, err := New(context.Background(), utConf)
	assert.Regexp(t, "FF00206", err)
}

func TestMTLSClientWithServer(t *testing.T) {
	// Create an X509 certificate pair
	privatekey, _ := rsa.GenerateKey(rand.Reader, 2048)
	publickey := &privatekey.PublicKey
	var privateKeyBytes []byte = x509.MarshalPKCS1PrivateKey(privatekey)
	privateKeyFile, _ := os.CreateTemp("", "key.pem")
	defer os.Remove(privateKeyFile.Name())
	privateKeyBlock := &pem.Block{Type: "RSA PRIVATE KEY", Bytes: privateKeyBytes}
	pem.Encode(privateKeyFile, privateKeyBlock)
	serialNumber, _ := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	x509Template := &x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			Organization: []string{"Unit Tests"},
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(100 * time.Second),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IPAddresses:           []net.IP{net.IPv4(127, 0, 0, 1)},
	}
	derBytes, err := x509.CreateCertificate(rand.Reader, x509Template, x509Template, publickey, privatekey)
	assert.NoError(t, err)
	publicKeyFile, _ := os.CreateTemp("", "cert.pem")
	defer os.Remove(publicKeyFile.Name())
	pem.Encode(publicKeyFile, &pem.Block{Type: "CERTIFICATE", Bytes: derBytes})

	http.HandleFunc("/hello", func(res http.ResponseWriter, req *http.Request) {
		res.WriteHeader(200)
		json.NewEncoder(res).Encode(map[string]interface{}{"hello": "world"})
	})

	// Create a CA certificate pool and add cert.pem to it
	caCert, err := os.ReadFile(publicKeyFile.Name())
	if err != nil {
		log.Fatal(err)
	}
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)

	// Create the TLS Config with the CA pool and enable Client certificate validation
	tlsConfig := &tls.Config{
		ClientCAs:  caCertPool,
		ClientAuth: tls.RequireAndVerifyClientCert,
	}
	tlsConfig.BuildNameToCertificate()

	// Create a Server instance to listen on port 8443 with the TLS config
	server := &http.Server{
		Addr:      "127.0.0.1:8443",
		TLSConfig: tlsConfig,
	}

	ctx, cancelCtx := context.WithCancel(context.Background())
	go func() {
		select {
		case <-ctx.Done():
			shutdownContext, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			if err := server.Shutdown(shutdownContext); err != nil {
				return
			}
		}
	}()

	go server.ListenAndServeTLS(publicKeyFile.Name(), privateKeyFile.Name())

	// Use ffresty to test the mTLS client as well
	var restyConfig = config.RootSection("resty")
	InitConfig(restyConfig)
	clientTLSSection := restyConfig.SubSection("tls")
	restyConfig.Set(HTTPConfigURL, "https://127.0.0.1")
	clientTLSSection.Set(fftls.HTTPConfTLSEnabled, true)
	clientTLSSection.Set(fftls.HTTPConfTLSKeyFile, privateKeyFile.Name())
	clientTLSSection.Set(fftls.HTTPConfTLSCertFile, publicKeyFile.Name())
	clientTLSSection.Set(fftls.HTTPConfTLSCAFile, publicKeyFile.Name())

	c, err := New(context.Background(), restyConfig)
	assert.Nil(t, err)

	//httpsAddr := fmt.Sprintf("https://localhost:8443/hello", server.Addr)
	res, err := c.R().Get("https://127.0.0.1:8443/hello")
	assert.NoError(t, err)

	assert.NoError(t, err)
	if res != nil {
		assert.Equal(t, 200, res.StatusCode())
		var resBody map[string]interface{}
		err = json.Unmarshal(res.Body(), &resBody)
		assert.NoError(t, err)
		assert.Equal(t, "world", resBody["hello"])
	}
	cancelCtx()
}
