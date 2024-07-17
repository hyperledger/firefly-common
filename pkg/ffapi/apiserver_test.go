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

package ffapi

import (
	"context"
	"fmt"
	"github.com/getkin/kin-openapi/openapi3"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/go-resty/resty/v2"
	"github.com/hyperledger/firefly-common/pkg/config"
	"github.com/hyperledger/firefly-common/pkg/httpserver"
	"github.com/hyperledger/firefly-common/pkg/metric"
	"github.com/stretchr/testify/assert"
)

// utManager simulate the type you'd pass through your request interface
type utManager struct {
	t                   *testing.T
	mockErr             error
	mockEnrichErr       error
	calledJSONHandler   string
	calledUploadHandler string
	calledStreamHandler string
}

type sampleInput struct {
	Input1 string `json:"input1"`
}

type sampleOutput struct {
	Output1 string `json:"output1"`
}

// utAPIRoute1 is a simple demonstration route
var utAPIRoute1 = &Route{
	Name:        "utAPIRoute1",
	Path:        "ut/utresource/{resourceid}/postit",
	Method:      http.MethodPost,
	Description: "simulated rather random POST route good for testing", /* should be translated in real app */
	PathParams: []*PathParam{
		{Name: "resourceid", Description: "My resource" /* should be translated in real app */},
	},
	FormParams: []*FormParam{
		{Name: "test_field"},
	},
	JSONInputValue:  func() interface{} { return &sampleInput{} },
	JSONOutputValue: func() interface{} { return &sampleOutput{} },
	JSONOutputCodes: []int{http.StatusOK},
	Extensions: &APIServerRouteExt[*utManager]{
		JSONHandler: func(r *APIRequest, um *utManager) (output interface{}, err error) {
			um.calledJSONHandler = r.PP["resourceid"]
			assert.Equal(um.t, "test_json_input", r.Input.(*sampleInput).Input1)
			return &sampleOutput{Output1: "test_json_output"}, um.mockErr
		},
		UploadHandler: func(r *APIRequest, um *utManager) (output interface{}, err error) {
			um.calledJSONHandler = r.PP["resourceid"]
			assert.Equal(um.t, "test_form_input", r.FP["test_field"])
			d, err := io.ReadAll(r.Part.Data)
			assert.NoError(um.t, err)
			assert.Equal(um.t, "test_form_data", string(d))
			return &sampleOutput{Output1: "test_form_output"}, um.mockErr
		},
	},
}

var utAPIRoute2 = &Route{
	Name:        "utAPIRoute2",
	Path:        "ut/utresource/{resourceid}/getit",
	Method:      http.MethodGet,
	Description: "random GET stream route for testing",
	PathParams: []*PathParam{
		{Name: "resourceid", Description: "My resource"},
	},
	FormParams:      nil,
	JSONInputValue:  nil,
	JSONOutputValue: nil,
	JSONOutputCodes: nil,
	CustomResponseRefs: map[string]*openapi3.ResponseRef{
		"200": {
			Value: &openapi3.Response{
				Content: openapi3.Content{
					"application/octet-stream": {},
				},
			},
		},
	},
	Extensions: &APIServerRouteExt[*utManager]{
		StreamHandler: func(r *APIRequest, um *utManager) (output io.ReadCloser, err error) {
			um.calledStreamHandler = r.PP["resourceid"]
			return io.NopCloser(strings.NewReader("a stream!")), nil
		},
	},
}

func initUTConfig() (config.Section, config.Section, config.Section) {
	config.RootConfigReset()
	apiConfig := config.RootSection("ut.api")
	metricsConfig := config.RootSection("ut.metrics")
	corsConfig := config.RootSection("ut.cors")
	InitAPIServerConfig(apiConfig, metricsConfig, corsConfig)
	apiConfig.Set(httpserver.HTTPConfPort, 0)
	metricsConfig.Set(httpserver.HTTPConfPort, 0)
	return apiConfig, metricsConfig, corsConfig
}

func newTestAPIServer(t *testing.T, start bool) (*utManager, *apiServer[*utManager], func()) {
	ctx, cancelCtx := context.WithCancel(context.Background())
	apiConfig, metricsConfig, corsConfig := initUTConfig()
	um := &utManager{t: t}
	as := NewAPIServer(ctx, APIServerOptions[*utManager]{
		MetricsRegistry: metric.NewPrometheusMetricsRegistry("ut"),
		Routes:          []*Route{utAPIRoute1, utAPIRoute2},
		EnrichRequest: func(r *APIRequest) (*utManager, error) {
			// This could be some dynamic object based on extra processing in the request,
			// but the most common case is you just have a "manager" that you inject into each
			// request and that's the "T" on the APIServer
			return um, um.mockEnrichErr
		},
		Description:   "unit testing",
		APIConfig:     apiConfig,
		MetricsConfig: metricsConfig,
		CORSConfig:    corsConfig,
	})
	done := make(chan struct{})
	if start {
		go func() {
			err := as.Serve(ctx)
			assert.NoError(t, err)
			close(done)
		}()
	} else {
		close(done)
	}
	return um, as.(*apiServer[*utManager]), func() {
		cancelCtx()
		<-done
	}
}

func TestAPIServerInvokeAPIRouteStream(t *testing.T) {
	um, as, done := newTestAPIServer(t, true)
	defer done()

	<-as.Started()

	var o sampleOutput
	res, err := resty.New().R().
		SetBody(nil).
		SetResult(&o).
		Get(fmt.Sprintf("%s/api/v1/ut/utresource/id12345/getit", as.APIPublicURL()))
	assert.NoError(t, err)
	assert.Equal(t, 200, res.StatusCode())
	assert.Equal(t, "application/octet-stream", res.Header().Get("Content-Type"))
	assert.Equal(t, "id12345", um.calledStreamHandler)
	assert.Equal(t, "a stream!", string(res.Body()))
}

func TestAPIServerInvokeAPIRouteJSON(t *testing.T) {
	um, as, done := newTestAPIServer(t, true)
	defer done()

	<-as.Started()

	var o sampleOutput
	res, err := resty.New().R().
		SetBody(&sampleInput{
			Input1: "test_json_input",
		}).
		SetResult(&o).
		Post(fmt.Sprintf("%s/api/v1/ut/utresource/id12345/postit", as.APIPublicURL()))
	assert.NoError(t, err)
	assert.Equal(t, 200, res.StatusCode())
	assert.Equal(t, "id12345", um.calledJSONHandler)
	assert.Equal(t, "test_json_output", o.Output1)
}

func TestAPIServerInvokeAPIRouteYAML(t *testing.T) {
	um, as, done := newTestAPIServer(t, true)
	defer done()
	as.handleYAML = true

	<-as.Started()

	var o sampleOutput
	res, err := resty.New().R().
		SetBody(`input1: test_json_input`).
		SetHeader("Content-Type", "application/x-yaml").
		SetResult(&o).
		Post(fmt.Sprintf("%s/api/v1/ut/utresource/id12345/postit", as.APIPublicURL()))
	assert.NoError(t, err)
	assert.Equal(t, 200, res.StatusCode())
	assert.Equal(t, "id12345", um.calledJSONHandler)
	assert.Equal(t, "test_json_output", o.Output1)
}

func TestAPIServerInvokeAPIRouteForm(t *testing.T) {
	um, as, done := newTestAPIServer(t, true)
	defer done()

	<-as.Started()

	var o sampleOutput
	res, err := resty.New().R().
		SetMultipartFormData(map[string]string{
			"test_field": "test_form_input",
		}).
		SetMultipartField("data", "file1", "text.plain", strings.NewReader("test_form_data")).
		SetResult(&o).
		Post(fmt.Sprintf("%s/api/v1/ut/utresource/id12345/postit", as.APIPublicURL()))
	assert.NoError(t, err)
	assert.Equal(t, 200, res.StatusCode())
	assert.Equal(t, "id12345", um.calledJSONHandler)
	assert.Equal(t, "test_form_output", o.Output1)

}
func TestAPIServerInvokeAPIRouteYAMLFail(t *testing.T) {
	_, as, done := newTestAPIServer(t, true)
	defer done()
	as.handleYAML = true

	<-as.Started()

	var o sampleOutput
	res, err := resty.New().R().
		SetBody(`{{{ !!! not yaml`).
		SetHeader("Content-Type", "application/x-yaml").
		SetResult(&o).
		Post(fmt.Sprintf("%s/api/v1/ut/utresource/id12345/postit", as.APIPublicURL()))
	assert.NoError(t, err)
	assert.Equal(t, 400, res.StatusCode())
}

func TestAPIServerInvokeAPIRouteCheckPathEscaping(t *testing.T) {
	um, as, done := newTestAPIServer(t, true)
	defer done()

	<-as.Started()

	var o sampleOutput
	res, err := resty.New().R().
		SetBody(&sampleInput{
			Input1: "test_json_input",
		}).
		SetResult(&o).
		Post(fmt.Sprintf("%s/api/v1/ut/utresource/foo%%2Fbar/postit", as.APIPublicURL()))
	assert.NoError(t, err)
	assert.Equal(t, 200, res.StatusCode())
	assert.Equal(t, "foo/bar", um.calledJSONHandler)
	assert.Equal(t, "test_json_output", o.Output1)
}

func TestAPIServerSwaggerJSON(t *testing.T) {
	_, as, done := newTestAPIServer(t, true)
	defer done()

	<-as.Started()

	res, err := resty.New().R().
		SetDoNotParseResponse(true).
		Put(fmt.Sprintf("%s/api/swagger.json", as.APIPublicURL()))
	assert.NoError(t, err)
	assert.Equal(t, 200, res.StatusCode())
	assert.Regexp(t, "application/json", res.Header().Get("content-type"))
}

func TestAPIServerSwaggerYAML(t *testing.T) {
	_, as, done := newTestAPIServer(t, true)
	defer done()

	<-as.Started()

	res, err := resty.New().R().
		SetDoNotParseResponse(true).
		Put(fmt.Sprintf("%s/api/swagger.yaml", as.APIPublicURL()))
	assert.NoError(t, err)
	assert.Equal(t, 200, res.StatusCode())
	assert.Regexp(t, "application/x-yaml", res.Header().Get("content-type"))
}

func TestAPIServerSwaggerUI(t *testing.T) {
	_, as, done := newTestAPIServer(t, true)
	defer done()

	<-as.Started()

	res, err := resty.New().R().
		SetDoNotParseResponse(true).
		Put(fmt.Sprintf("%s/api", as.APIPublicURL()))
	assert.NoError(t, err)
	assert.Equal(t, 200, res.StatusCode())
	assert.Regexp(t, "text/html", res.Header().Get("content-type"))
}

func TestAPIServerInvokeEnrichFailJSON(t *testing.T) {
	um, as, done := newTestAPIServer(t, true)
	defer done()

	um.mockEnrichErr = fmt.Errorf("pop")
	<-as.Started()

	res, err := resty.New().R().
		SetBody(&sampleInput{
			Input1: "test_json_input",
		}).
		Post(fmt.Sprintf("%s/api/v1/ut/utresource/id12345/postit", as.APIPublicURL()))
	assert.NoError(t, err)
	assert.Equal(t, 500, res.StatusCode())
}

func TestAPIServerInvokeEnrichFailForm(t *testing.T) {
	um, as, done := newTestAPIServer(t, true)
	defer done()

	um.mockEnrichErr = fmt.Errorf("pop")
	<-as.Started()

	res, err := resty.New().R().
		SetMultipartField("data", "file1", "text.plain", strings.NewReader("test_form_data")).
		Post(fmt.Sprintf("%s/api/v1/ut/utresource/id12345/postit", as.APIPublicURL()))
	assert.NoError(t, err)
	assert.Equal(t, 500, res.StatusCode())
}

func TestAPIServer404(t *testing.T) {
	_, as, done := newTestAPIServer(t, true)
	defer done()

	<-as.Started()

	res, err := resty.New().R().
		SetDoNotParseResponse(true).
		Put(fmt.Sprintf("%s/wrong", as.APIPublicURL()))
	assert.NoError(t, err)
	assert.Equal(t, 404, res.StatusCode())
}

func TestAPIServer500(t *testing.T) {
	um, as, done := newTestAPIServer(t, true)
	defer done()
	um.mockErr = fmt.Errorf("pop")

	<-as.Started()

	res, err := resty.New().R().
		SetBody(&sampleInput{
			Input1: "test_json_input",
		}).
		Post(fmt.Sprintf("%s/api/v1/ut/utresource/id12345/postit", as.APIPublicURL()))
	assert.NoError(t, err)
	assert.Equal(t, 500, res.StatusCode())
}

func TestAPIServerFailServe(t *testing.T) {
	_, as, done := newTestAPIServer(t, false)
	defer done()

	as.APIConfig.Set(httpserver.HTTPConfAddress, "!badness")
	err := as.Serve(context.Background())
	assert.Regexp(t, "FF00151", err)

	// Check we still closed the started channel
	<-as.Started()

}

func TestAPIServerFailServeMetrics(t *testing.T) {
	_, as, done := newTestAPIServer(t, false)
	defer done()

	as.MetricsConfig.Set(httpserver.HTTPConfAddress, "!badness")
	err := as.Serve(context.Background())
	assert.Regexp(t, "FF00151", err)

	// Check we still closed the started channel
	<-as.Started()

}

func TestWaitForServerStop(t *testing.T) {
	_, as, done := newTestAPIServer(t, false)
	defer done()

	open1 := make(chan error)
	closed1 := make(chan error)
	close(closed1)
	as.waitForServerStop(open1, closed1)
	as.waitForServerStop(closed1, open1)

}

func TestBadRoute(t *testing.T) {
	apiConfig, metricsConfig, corsConfig := initUTConfig()
	as := NewAPIServer(context.Background(), APIServerOptions[*utManager]{
		MetricsRegistry: metric.NewPrometheusMetricsRegistry("ut"),
		Routes: []*Route{{
			Extensions: &APIServerRouteExt[string]{}, // T does not match *utManager
		}},
		APIConfig:     apiConfig,
		MetricsConfig: metricsConfig,
		CORSConfig:    corsConfig,
	})
	assert.Panics(t, func() { as.Serve(context.Background()) })
}

func TestBadMetrics(t *testing.T) {
	_, as, done := newTestAPIServer(t, false)
	defer done()
	as.MetricsRegistry = metric.NewPrometheusMetricsRegistry("wrong")
	assert.Panics(t, func() { as.createMetricsMuxRouter(context.Background()) })
}
