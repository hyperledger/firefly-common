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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/hyperledger/firefly-common/pkg/config"
	"github.com/hyperledger/firefly-common/pkg/httpserver"
	"github.com/hyperledger/firefly-common/pkg/i18n"
	"github.com/stretchr/testify/assert"
)

const configDir = "../../test/data/config"

func newTestHandlerFactory() *HandlerFactory {
	return &HandlerFactory{
		DefaultRequestTimeout: 5 * time.Second,
		PassthroughHeaders: []string{
			"X-Custom-Header",
		},
	}
}

func newTestServer(t *testing.T, routes []*Route) (httpserver.HTTPServer, *mux.Router, func()) {
	r := mux.NewRouter()
	hs := newTestHandlerFactory()
	for _, route := range routes {
		r.HandleFunc(route.Path, hs.RouteHandler(route)).Methods(route.Method)
	}

	done := make(chan error)
	ctx, cancel := context.WithCancel(context.Background())
	config.RootConfigReset()
	conf := config.RootSection("ut")
	httpserver.InitHTTPConfig(conf, 0)
	httpserver.InitCORSConfig(conf)
	s, err := httpserver.NewHTTPServer(ctx, "ut", r, done, conf, conf)
	assert.NoError(t, err)

	go s.ServeHTTP(ctx)

	return s, r, func() {
		cancel()
		<-done
	}
}

func TestRouteServePOST201WithParams(t *testing.T) {
	s, _, done := newTestServer(t, []*Route{{
		Name:   "testRoute",
		Path:   "/test/{something}",
		Method: "POST",
		PathParams: []*PathParam{
			{Name: "something"},
		},
		QueryParams: []*QueryParam{
			{Name: "param1"},
			{Name: "param2", IsBool: true},
			{Name: "param3", IsBool: true},
		},
		JSONInputValue:  func() interface{} { return make(map[string]interface{}) },
		JSONOutputValue: func() interface{} { return make(map[string]interface{}) },
		JSONOutputCodes: []int{201},
		JSONHandler: func(r *APIRequest) (output interface{}, err error) {
			assert.Equal(t, "stuff", r.PP["something"])
			assert.Equal(t, "thing", r.QP["param1"])
			assert.Equal(t, "true", r.QP["param2"])
			assert.Equal(t, "false", r.QP["param3"])
			return map[string]interface{}{"output1": "value2"}, nil
		},
	}})
	defer done()

	b, _ := json.Marshal(map[string]interface{}{"input1": "value1"})
	res, err := http.Post(fmt.Sprintf("http://%s/test/stuff?param1=thing&param2&param3=false", s.Addr()), "application/json", bytes.NewReader(b))
	assert.NoError(t, err)
	assert.Equal(t, 201, res.StatusCode)
	var resJSON map[string]interface{}
	json.NewDecoder(res.Body).Decode(&resJSON)
	assert.Equal(t, "value2", resJSON["output1"])
}

func TestJSONHTTPResponseEncodeFail(t *testing.T) {
	s, _, done := newTestServer(t, []*Route{{
		Name:            "testRoute",
		Path:            "/test",
		Method:          "POST",
		JSONInputValue:  nil,
		JSONOutputValue: func() interface{} { return make(map[string]interface{}) },
		JSONOutputCodes: []int{200},
		JSONHandler: func(r *APIRequest) (output interface{}, err error) {
			v := map[string]interface{}{"unserializable": map[bool]interface{}{true: "not in JSON"}}
			return v, nil
		},
	}})
	defer done()

	b, _ := json.Marshal(map[string]interface{}{"input1": "value1"})
	res, err := http.Post(fmt.Sprintf("http://%s/test", s.Addr()), "application/json", bytes.NewReader(b))
	assert.NoError(t, err)
	var resJSON map[string]interface{}
	json.NewDecoder(res.Body).Decode(&resJSON)
	assert.Regexp(t, "FF00165", resJSON["error"])
}

func TestJSONHTTPNilResponseNon204(t *testing.T) {
	s, _, done := newTestServer(t, []*Route{{
		Name:            "testRoute",
		Path:            "/test",
		Method:          "POST",
		JSONInputValue:  nil,
		JSONOutputValue: func() interface{} { return make(map[string]interface{}) },
		JSONOutputCodes: []int{200},
		JSONHandler: func(r *APIRequest) (output interface{}, err error) {
			return nil, nil
		},
	}})
	defer done()

	b, _ := json.Marshal(map[string]interface{}{"input1": "value1"})
	res, err := http.Post(fmt.Sprintf("http://%s/test", s.Addr()), "application/json", bytes.NewReader(b))
	assert.NoError(t, err)
	assert.Equal(t, 404, res.StatusCode)
	var resJSON map[string]interface{}
	json.NewDecoder(res.Body).Decode(&resJSON)
	assert.Regexp(t, "FF00164", resJSON["error"])
}

func TestJSONHTTPDefault500Error(t *testing.T) {
	s, _, done := newTestServer(t, []*Route{{
		Name:            "testRoute",
		Path:            "/test",
		Method:          "POST",
		JSONInputValue:  nil,
		JSONOutputValue: func() interface{} { return make(map[string]interface{}) },
		JSONOutputCodes: []int{200},
		JSONHandler: func(r *APIRequest) (output interface{}, err error) {
			return nil, fmt.Errorf("pop")
		},
	}})
	defer done()

	b, _ := json.Marshal(map[string]interface{}{"input1": "value1"})
	res, err := http.Post(fmt.Sprintf("http://%s/test", s.Addr()), "application/json", bytes.NewReader(b))
	assert.NoError(t, err)
	assert.Equal(t, 500, res.StatusCode)
	var resJSON map[string]interface{}
	json.NewDecoder(res.Body).Decode(&resJSON)
	assert.Regexp(t, "pop", resJSON["error"])
}

func TestStatusCodeHintMapping(t *testing.T) {
	s, _, done := newTestServer(t, []*Route{{
		Name:            "testRoute",
		Path:            "/test",
		Method:          "POST",
		JSONInputValue:  nil,
		JSONOutputValue: func() interface{} { return make(map[string]interface{}) },
		JSONOutputCodes: []int{200},
		JSONHandler: func(r *APIRequest) (output interface{}, err error) {
			return nil, i18n.NewError(r.Req.Context(), i18n.MsgResponseMarshalError)
		},
	}})
	defer done()

	b, _ := json.Marshal(map[string]interface{}{"input1": "value1"})
	res, err := http.Post(fmt.Sprintf("http://%s/test", s.Addr()), "application/json", bytes.NewReader(b))
	assert.NoError(t, err)
	assert.Equal(t, 400, res.StatusCode)
	var resJSON map[string]interface{}
	json.NewDecoder(res.Body).Decode(&resJSON)
	assert.Regexp(t, "FF00165", resJSON["error"])
}

func TestFilter(t *testing.T) {
	s, _, done := newTestServer(t, []*Route{{
		Name:            "testRoute",
		Path:            "/test",
		Method:          "GET",
		FilterFactory:   TestQueryFactory,
		JSONInputValue:  nil,
		JSONOutputValue: func() interface{} { return []string{} },
		JSONOutputCodes: []int{200},
		JSONHandler: func(r *APIRequest) (output interface{}, err error) {
			assert.NotNil(t, r.Filter)
			return r.FilterResult([]string{"test"}, nil, nil)
		},
	}})
	defer done()

	res, err := http.Get(fmt.Sprintf("http://%s/test?id=1234", s.Addr()))
	assert.NoError(t, err)
	assert.Equal(t, 200, res.StatusCode)
	var resJSON []string
	json.NewDecoder(res.Body).Decode(&resJSON)
	assert.Equal(t, []string{"test"}, resJSON)
}

func TestStatusInvalidContentType(t *testing.T) {
	s, _, done := newTestServer(t, []*Route{{
		Name:            "testRoute",
		Path:            "/test",
		Method:          "POST",
		JSONInputValue:  nil,
		JSONOutputValue: func() interface{} { return make(map[string]interface{}) },
		JSONOutputCodes: []int{204},
		JSONHandler: func(r *APIRequest) (output interface{}, err error) {
			return nil, nil
		},
	}})
	defer done()

	res, err := http.Post(fmt.Sprintf("http://%s/test", s.Addr()), "application/text", bytes.NewReader([]byte{}))
	assert.NoError(t, err)
	assert.Equal(t, 415, res.StatusCode)
	var resJSON map[string]interface{}
	json.NewDecoder(res.Body).Decode(&resJSON)
	assert.Regexp(t, "FF00162", resJSON["error"])
}

func TestTimeout(t *testing.T) {
	s, _, done := newTestServer(t, []*Route{{
		Name:            "testRoute",
		Path:            "/test",
		Method:          "GET",
		JSONInputValue:  nil,
		JSONOutputValue: func() interface{} { return make(map[string]interface{}) },
		JSONOutputCodes: []int{204},
		JSONHandler: func(r *APIRequest) (output interface{}, err error) {
			<-r.Req.Context().Done()
			return nil, fmt.Errorf("timeout error")
		},
	}})
	defer done()
	req, err := http.NewRequest("GET", fmt.Sprintf("http://%s/test", s.Addr()), bytes.NewReader([]byte(``)))
	assert.NoError(t, err)
	req.Header.Set("Request-Timeout", "250us")
	res, err := http.DefaultClient.Do(req)
	assert.NoError(t, err)
	assert.Equal(t, 408, res.StatusCode)
	var resJSON map[string]interface{}
	json.NewDecoder(res.Body).Decode(&resJSON)
	assert.Regexp(t, "FF00166.*timeout error", resJSON["error"])
}

func TestBadTimeout(t *testing.T) {
	s, _, done := newTestServer(t, []*Route{{
		Name:            "testRoute",
		Path:            "/test",
		Method:          "GET",
		JSONInputValue:  nil,
		JSONOutputValue: func() interface{} { return make(map[string]interface{}) },
		JSONOutputCodes: []int{204},
		JSONHandler: func(r *APIRequest) (output interface{}, err error) {
			return nil, nil
		},
	}})
	defer done()
	req, err := http.NewRequest("GET", fmt.Sprintf("http://%s/test", s.Addr()), bytes.NewReader([]byte(``)))
	assert.NoError(t, err)
	req.Header.Set("Request-Timeout", "bad timeout")
	res, err := http.DefaultClient.Do(req)
	assert.NoError(t, err)
	assert.Equal(t, 204, res.StatusCode)
}

func TestMultipartBinary(t *testing.T) {
	s, _, done := newTestServer(t, []*Route{{
		Name:   "testRoute",
		Path:   "/test",
		Method: "POST",
		FormParams: []*FormParam{
			{Name: "param1", Description: ExampleDesc},
		},
		JSONInputValue:  nil,
		JSONOutputValue: func() interface{} { return make(map[string]interface{}) },
		JSONOutputCodes: []int{http.StatusCreated},
		FormUploadHandler: func(r *APIRequest) (output interface{}, err error) {
			assert.Equal(t, "testing", r.FP["param1"])
			d, err := ioutil.ReadAll(r.Part.Data)
			assert.NoError(t, err)
			assert.Equal(t, "some data", string(d))
			return ioutil.NopCloser(strings.NewReader(`{"ok":true}`)), nil
		},
	}})
	defer done()

	b := new(bytes.Buffer)
	w := multipart.NewWriter(b)
	writer, err := w.CreateFormField("param1")
	assert.NoError(t, err)
	writer.Write([]byte(`testing`))
	writer, err = w.CreateFormFile("file", "filename.ext")
	assert.NoError(t, err)
	writer.Write([]byte(`some data`))
	w.Close()

	req, err := http.NewRequest("POST", fmt.Sprintf("http://%s/test", s.Addr()), b)
	assert.NoError(t, err)
	req.Header.Set("Content-Type", w.FormDataContentType())
	res, err := http.DefaultClient.Do(req)
	assert.NoError(t, err)
	assert.Equal(t, 201, res.StatusCode)
	bodyBytes, err := ioutil.ReadAll(res.Body)
	assert.NoError(t, err)
	assert.JSONEq(t, `{"ok":true}`, string(bodyBytes))
}

func TestMultipartBinaryFieldAfterBinary(t *testing.T) {
	s, _, done := newTestServer(t, []*Route{{
		Name:   "testRoute",
		Path:   "/test",
		Method: "POST",
		FormParams: []*FormParam{
			{Name: "param1", Description: ExampleDesc},
		},
		JSONInputValue:  nil,
		JSONOutputValue: func() interface{} { return make(map[string]interface{}) },
		JSONOutputCodes: []int{http.StatusCreated},
		FormUploadHandler: func(r *APIRequest) (output interface{}, err error) {
			return nil, nil
		},
	}})
	defer done()

	b := new(bytes.Buffer)
	w := multipart.NewWriter(b)
	writer, err := w.CreateFormFile("file", "filename.ext")
	assert.NoError(t, err)
	writer, err = w.CreateFormField("param1")
	assert.NoError(t, err)
	writer.Write([]byte(`testing`))
	writer.Write([]byte(`some data`))
	w.Close()

	req, err := http.NewRequest("POST", fmt.Sprintf("http://%s/test", s.Addr()), b)
	assert.NoError(t, err)
	req.Header.Set("Content-Type", w.FormDataContentType())
	res, err := http.DefaultClient.Do(req)
	assert.NoError(t, err)
	assert.Equal(t, 400, res.StatusCode)
	var resJSON map[string]interface{}
	json.NewDecoder(res.Body).Decode(&resJSON)
	assert.Regexp(t, "FF00163", resJSON["error"])
}

func TestMultipartBadData(t *testing.T) {
	s, _, done := newTestServer(t, []*Route{{
		Name:   "testRoute",
		Path:   "/test",
		Method: "POST",
		FormParams: []*FormParam{
			{Name: "param1", Description: ExampleDesc},
		},
		JSONInputValue:  nil,
		JSONOutputValue: func() interface{} { return make(map[string]interface{}) },
		JSONOutputCodes: []int{http.StatusCreated},
		FormUploadHandler: func(r *APIRequest) (output interface{}, err error) {
			return nil, nil
		},
	}})
	defer done()

	b := new(bytes.Buffer)
	w := multipart.NewWriter(b)
	w.Close()

	req, err := http.NewRequest("POST", fmt.Sprintf("http://%s/test", s.Addr()), strings.NewReader(`this is no form data`))
	assert.NoError(t, err)
	req.Header.Set("Content-Type", w.FormDataContentType())
	res, err := http.DefaultClient.Do(req)
	assert.NoError(t, err)
	assert.Equal(t, 400, res.StatusCode)
	var resJSON map[string]interface{}
	json.NewDecoder(res.Body).Decode(&resJSON)
	assert.Regexp(t, "FF00161", resJSON["error"])
}

func TestMultipartBadContentType(t *testing.T) {
	hf := newTestHandlerFactory()
	_, err := hf.getFilePart(httptest.NewRequest("GET", "/wrong", nil))
	assert.Regexp(t, "FF00161", err)
}

func TestSwaggerUI(t *testing.T) {
	hf := newTestHandlerFactory()
	h := hf.SwaggerUIHandler("http://localhost:5000/api/v1")

	res := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/api/v1", nil)
	status, err := h(res, req)
	assert.Equal(t, 200, status)
	assert.NoError(t, err)
}

func TestGetTimeoutMax(t *testing.T) {
	hf := newTestHandlerFactory()
	hf.MaxTimeout = 1 * time.Second
	req, err := http.NewRequest("GET", "http://test.example.com", bytes.NewReader([]byte(``)))
	req.Header.Set("Request-Timeout", "1h")
	assert.NoError(t, err)
	timeout := hf.getTimeout(req)
	assert.Equal(t, 1*time.Second, timeout)
}

func TestCustomHeaderPassthrough(t *testing.T) {
	s, _, done := newTestServer(t, []*Route{{
		Name:            "testRoute",
		Path:            "/test/{something}",
		Method:          "POST",
		PathParams:      []*PathParam{},
		QueryParams:     []*QueryParam{},
		JSONInputValue:  func() interface{} { return make(map[string]interface{}) },
		JSONOutputValue: func() interface{} { return make(map[string]interface{}) },
		JSONOutputCodes: []int{201},
		JSONHandler: func(r *APIRequest) (output interface{}, err error) {
			ctx := r.Req.Context()
			headers := ctx.Value(CtxHeadersKey{}).(http.Header)
			assert.Equal(t, headers.Get("X-Custom-Header"), "custom value")
			return map[string]interface{}{"output1": "value2"}, nil
		},
	}})
	defer done()

	b, _ := json.Marshal(map[string]interface{}{"input1": "value1"})
	req, err := http.NewRequest("POST", fmt.Sprintf("http://%s/test/stuff", s.Addr()), bytes.NewReader(b))
	req.Header.Set("Content-type", "application/json")
	req.Header.Set("X-Custom-Header", "custom value")
	assert.NoError(t, err)
	res, err := http.DefaultClient.Do(req)
	assert.NoError(t, err)
	assert.Equal(t, 201, res.StatusCode)
	var resJSON map[string]interface{}
	json.NewDecoder(res.Body).Decode(&resJSON)
	assert.Equal(t, "value2", resJSON["output1"])
}
