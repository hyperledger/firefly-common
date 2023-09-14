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

package ffapi

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-resty/resty/v2"
	"github.com/stretchr/testify/assert"
)

func TestOpenAPI3SwaggerUI(t *testing.T) {
	oaf := &OpenAPIHandlerFactory{
		BaseSwaggerGenOptions: SwaggerGenOptions{},
		StaticPublicURL:       "http://localhost:12345/basepath",
	}
	testServer := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		status, err := oaf.SwaggerUIHandler("/api/myswagger.json")(res, req)
		assert.NoError(t, err)
		assert.Equal(t, 200, status)
	}))
	defer testServer.Close()

	res, err := resty.New().R().SetDoNotParseResponse(true).Get(testServer.URL)
	assert.NoError(t, err)
	assert.True(t, res.IsSuccess())
	body := res.RawBody()
	defer body.Close()
	b, err := io.ReadAll(body)
	assert.NoError(t, err)
	assert.Contains(t, string(b), "http://localhost:12345/basepath/api/myswagger.json")
}

func TestOpenAPI3SwaggerUIDynamicPublicURLHeader(t *testing.T) {
	oaf := &OpenAPIHandlerFactory{
		BaseSwaggerGenOptions:  SwaggerGenOptions{},
		DynamicPublicURLHeader: "X-External-URL",
	}
	testServer := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		status, err := oaf.SwaggerUIHandler("/api/myswagger.json")(res, req)
		assert.NoError(t, err)
		assert.Equal(t, 200, status)
	}))
	defer testServer.Close()

	res, err := resty.New().R().
		SetHeader("X-External-URL", "https://example.host.com/a/path/").
		SetDoNotParseResponse(true).Get(testServer.URL)
	assert.NoError(t, err)
	assert.True(t, res.IsSuccess())
	body := res.RawBody()
	defer body.Close()
	b, err := io.ReadAll(body)
	assert.NoError(t, err)
	assert.Contains(t, string(b), "https://example.host.com/a/path/api/myswagger.json")
}

func TestOpenAPI3SwaggerUIDynamicPublicURL(t *testing.T) {
	oaf := &OpenAPIHandlerFactory{
		BaseSwaggerGenOptions: SwaggerGenOptions{},
		DynamicPublicURLBuilder: func(req *http.Request) string {
			return fmt.Sprintf("https://%s/", req.Header.Get("X-Forwarded-Host"))
		},
	}
	testServer := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		status, err := oaf.SwaggerUIHandler("/api/myswagger.json")(res, req)
		assert.NoError(t, err)
		assert.Equal(t, 200, status)
	}))
	defer testServer.Close()

	res, err := resty.New().R().
		SetHeader("X-Forwarded-Host", "example.host.com:12345").
		SetDoNotParseResponse(true).Get(testServer.URL)
	assert.NoError(t, err)
	assert.True(t, res.IsSuccess())
	body := res.RawBody()
	defer body.Close()
	b, err := io.ReadAll(body)
	assert.NoError(t, err)
	assert.Contains(t, string(b), "https://example.host.com:12345/api/myswagger.json")
}
