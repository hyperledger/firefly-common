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

package ffapi

import (
	"context"
	"fmt"
	"net/http"
	"testing"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/ghodss/yaml"
	"github.com/hyperledger/firefly-common/pkg/config"
	"github.com/hyperledger/firefly-common/pkg/fftypes"
	"github.com/hyperledger/firefly-common/pkg/i18n"
	"github.com/stretchr/testify/assert"
	"golang.org/x/text/language"
)

type TestEnum fftypes.FFEnum

var (
	TestEnumVal1 = fftypes.FFEnumValue("ut", "val1")
	TestEnumVal2 = fftypes.FFEnumValue("ut", "val2")
)

type TestStruct1 struct {
	TestEmbedded1
	ID     *fftypes.UUID     `ffstruct:"ut1" json:"id"`
	Time   *fftypes.FFTime   `ffstruct:"ut1" json:"time"`
	Hash   *fftypes.Bytes32  `ffstruct:"ut1" json:"hash"`
	Amount *fftypes.FFBigInt `ffstruct:"ut1" json:"amount"`
}

type TestEmbedded1 struct {
	Enum1    TestEnum         `ffstruct:"ut1" json:"enum1" ffenum:"ut"`
	String1  string           `ffstruct:"ut1" json:"string1"`
	String2  string           `ffstruct:"ut1" json:"string2"`
	JSONAny1 *fftypes.JSONAny `ffstruct:"ut1" json:"jsonAny1,omitempty"`
	Number1  int64            `ffstruct:"ut1" json:"number1"`
	Struct2  TestStruct2      `ffstruct:"ut1" json:"struct2"`
}

type TestStruct2 struct {
	Enum1    TestEnum         `ffstruct:"ut1" json:"enum1" ffenum:"ut"`
	String1  string           `ffstruct:"ut1" json:"string1"`
	String2  string           `ffstruct:"ut1" json:"string2"`
	JSONAny1 *fftypes.JSONAny `ffstruct:"ut1" json:"jsonAny1,omitempty"`
}

var ExampleDesc = i18n.FFM(language.AmericanEnglish, "TestKey", "Test Description")

var testRoutes = []*Route{
	{
		Name:   "op1",
		Path:   "namespaces/{ns}/example1/{id}/things",
		Method: http.MethodGet,
		PathParams: []*PathParam{
			{Name: "lang", ExampleFromConf: config.Lang, Description: ExampleDesc},
			{Name: "id", Example: "id12345", Description: ExampleDesc},
		},
		FilterFactory:   TestQueryFactory,
		QueryParams:     nil,
		Description:     ExampleDesc,
		JSONInputValue:  func() interface{} { return &TestStruct1{} },
		JSONOutputValue: func() interface{} { return &TestStruct2{} },
		JSONOutputCodes: []int{http.StatusOK},
	},
	{
		Name:           "op2",
		Path:           "example2",
		Method:         http.MethodPatch,
		PathParams:     nil,
		QueryParams:    nil,
		Description:    ExampleDesc,
		JSONInputValue: func() interface{} { return nil },
		JSONInputSchema: func(ctx context.Context, schemaGen SchemaGenerator) (*openapi3.SchemaRef, error) {
			s1, _ := schemaGen(&TestStruct1{})
			s2, _ := schemaGen(&TestStruct2{})
			return &openapi3.SchemaRef{
				Value: openapi3.NewAnyOfSchema(s1.Value, s2.Value),
			}, nil
		},
		JSONOutputSchema: func(ctx context.Context, schemaGen SchemaGenerator) (*openapi3.SchemaRef, error) {
			return schemaGen(&TestStruct1{})
		},
		JSONOutputCodes: []int{http.StatusOK},
	},
	{
		Name:       "op3",
		Path:       "example2",
		Method:     http.MethodPut,
		PathParams: nil,
		QueryParams: []*QueryParam{
			{Name: "lang", ExampleFromConf: config.Lang, Description: ExampleDesc},
			{Name: "myfield", Default: "val1", Description: ExampleDesc},
		},
		Description:     ExampleDesc,
		JSONInputValue:  func() interface{} { return &TestStruct1{} },
		JSONOutputValue: func() interface{} { return nil },
		JSONOutputCodes: []int{http.StatusNoContent},
		FormParams: []*FormParam{
			{Name: "metadata", Description: ExampleDesc},
		},
		FormUploadHandler: func(r *APIRequest) (output interface{}, err error) { return nil, nil },
	},
	{
		Name:   "op4",
		Path:   "example2/{id}",
		Method: http.MethodDelete,
		PathParams: []*PathParam{
			{Name: "id", Description: ExampleDesc},
		},
		QueryParams:     nil,
		Description:     ExampleDesc,
		JSONInputValue:  func() interface{} { return nil },
		JSONOutputValue: func() interface{} { return nil },
		JSONOutputCodes: []int{http.StatusNoContent},
	},
	{
		Name:            "op5",
		Path:            "example2",
		Method:          http.MethodPost,
		PathParams:      nil,
		QueryParams:     nil,
		Description:     ExampleDesc,
		JSONInputValue:  func() interface{} { return &TestStruct1{} },
		JSONOutputValue: func() interface{} { return &TestStruct1{} },
		JSONOutputCodes: []int{http.StatusOK},
	},
}

type TestInOutType struct {
	Length           float64          `ffstruct:"TestInOutType" json:"length"`
	Width            float64          `ffstruct:"TestInOutType" json:"width"`
	Height           float64          `ffstruct:"TestInOutType" json:"height" ffexcludeoutput:"true"`
	Volume           fftypes.FFBigInt `ffstruct:"TestInOutType" json:"volume" ffexcludeinput:"true"`
	Secret           string           `ffstruct:"TestInOutType" json:"secret" ffexclude:"true"`
	Conditional      string           `ffstruct:"TestInOutType" json:"conditional" ffexclude:"PostTagTest"`
	ConditionalInput string           `ffstruct:"TestInOutType" json:"conditionalInput" ffexcludeinput:"PostTagTest"`
}

type TestNonTaggedType struct {
	NoFFStructTag string `json:"noFFStructTag"`
}

func TestOpenAPI3SwaggerGen(t *testing.T) {
	doc := NewSwaggerGen(&Options{
		Title:   "UnitTest",
		Version: "1.0",
		BaseURL: "http://localhost:12345/api/v1",
		RouteCustomizations: func(ctx context.Context, sg *SwaggerGen, route *Route, op *openapi3.Operation) {
			sg.AddParam(ctx, op, "header", "x-my-param", "thing", "stuff", ExampleDesc, false)
		},
		SupportFieldRedaction: true,
	}).Generate(context.Background(), testRoutes)
	err := doc.Validate(context.Background())
	assert.NoError(t, err)

	b, err := yaml.Marshal(doc)
	assert.NoError(t, err)
	fmt.Print(string(b))
}

func TestBadCustomInputSchemaFail(t *testing.T) {

	routes := []*Route{
		{
			Name:            "op6",
			Path:            "namespaces/{ns}/example1/{id}",
			Method:          http.MethodPost,
			JSONInputValue:  func() interface{} { return &TestStruct1{} },
			JSONInputMask:   []string{"id"},
			JSONOutputCodes: []int{http.StatusOK},
			JSONInputSchema: func(ctx context.Context, schemaGen SchemaGenerator) (*openapi3.SchemaRef, error) {
				return nil, fmt.Errorf("pop")
			},
		},
	}
	assert.Panics(t, func() {
		_ = NewSwaggerGen(&Options{
			Title:   "UnitTest",
			Version: "1.0",
			BaseURL: "http://localhost:12345/api/v1",
		}).Generate(context.Background(), routes)
	})
}

func TestBadCustomOutputSchemaFail(t *testing.T) {
	routes := []*Route{
		{
			Name:           "op7",
			Path:           "namespaces/{ns}/example1/{id}",
			Method:         http.MethodGet,
			JSONInputValue: func() interface{} { return &TestStruct1{} },
			JSONInputMask:  []string{"id"},
			JSONOutputSchema: func(ctx context.Context, schemaGen SchemaGenerator) (*openapi3.SchemaRef, error) {
				return nil, fmt.Errorf("pop")
			},
		},
	}
	assert.Panics(t, func() {
		_ = NewSwaggerGen(&Options{
			Title:   "UnitTest",
			Version: "1.0",
			BaseURL: "http://localhost:12345/api/v1",
		}).Generate(context.Background(), routes)
	})
}

func TestDuplicateOperationIDCheck(t *testing.T) {
	routes := []*Route{
		{Name: "op1"}, {Name: "op1"},
	}
	assert.PanicsWithValue(t, "Duplicate/invalid name (used as operation ID in swagger): op1", func() {
		_ = NewSwaggerGen(&Options{
			Title:   "UnitTest",
			Version: "1.0",
			BaseURL: "http://localhost:12345/api/v1",
		}).Generate(context.Background(), routes)
	})
}

func TestWildcards(t *testing.T) {
	routes := []*Route{
		{
			Name:            "op1",
			Path:            "namespaces/{ns}/example1/{id:.*wildcard.*}",
			Method:          http.MethodPost,
			JSONInputValue:  func() interface{} { return &TestStruct1{} },
			JSONOutputCodes: []int{http.StatusOK},
		},
	}
	swagger := NewSwaggerGen(&Options{
		Title:   "UnitTest",
		Version: "1.0",
		BaseURL: "http://localhost:12345/api/v1",
	}).Generate(context.Background(), routes)
	assert.NotNil(t, swagger.Paths["/namespaces/{ns}/example1/{id}"])
}

func TestFFExcludeTag(t *testing.T) {
	routes := []*Route{
		{
			Name:            "PostTagTest",
			Path:            "namespaces/{ns}/example1/test",
			Method:          http.MethodPost,
			JSONInputValue:  func() interface{} { return &TestInOutType{} },
			JSONOutputValue: func() interface{} { return &TestInOutType{} },
			JSONOutputCodes: []int{http.StatusOK},
		},
	}
	swagger := NewSwaggerGen(&Options{
		Title:   "UnitTest",
		Version: "1.0",
		BaseURL: "http://localhost:12345/api/v1",
	}).Generate(context.Background(), routes)
	assert.NotNil(t, swagger.Paths["/namespaces/{ns}/example1/test"].Post.RequestBody.Value)
	length, err := swagger.Paths["/namespaces/{ns}/example1/test"].Post.RequestBody.Value.Content.Get("application/json").Schema.Value.Properties.JSONLookup("length")
	assert.NoError(t, err)
	assert.NotNil(t, length)
	width, err := swagger.Paths["/namespaces/{ns}/example1/test"].Post.RequestBody.Value.Content.Get("application/json").Schema.Value.Properties.JSONLookup("width")
	assert.NoError(t, err)
	assert.NotNil(t, width)
	_, err = swagger.Paths["/namespaces/{ns}/example1/test"].Post.RequestBody.Value.Content.Get("application/json").Schema.Value.Properties.JSONLookup("secret")
	assert.Regexp(t, "object has no field", err)
	_, err = swagger.Paths["/namespaces/{ns}/example1/test"].Post.RequestBody.Value.Content.Get("application/json").Schema.Value.Properties.JSONLookup("conditional")
	assert.Regexp(t, "object has no field", err)
	_, err = swagger.Paths["/namespaces/{ns}/example1/test"].Post.RequestBody.Value.Content.Get("application/json").Schema.Value.Properties.JSONLookup("conditionalInput")
	assert.Regexp(t, "object has no field", err)
}

func TestPanicOnMissingDescription(t *testing.T) {
	routes := []*Route{
		{
			Name:            "PostPanicOnMissingDescription",
			Path:            "namespaces/{ns}/example1/test",
			Method:          http.MethodPost,
			Description:     "this is fine",
			JSONInputValue:  func() interface{} { return &TestInOutType{} },
			JSONOutputValue: func() interface{} { return &TestInOutType{} },
			JSONOutputCodes: []int{http.StatusOK},
		},
	}
	assert.PanicsWithValue(t, "invalid schema: FF00158: Field description missing for 'TestInOutType.conditional' on route 'PostPanicOnMissingDescription'", func() {
		_ = NewSwaggerGen(&Options{
			Title:                     "UnitTest",
			Version:                   "1.0",
			BaseURL:                   "http://localhost:12345/api/v1",
			PanicOnMissingDescription: true,
		}).Generate(context.Background(), routes)
	})
}

func TestPanicOnMissingFFStructTag(t *testing.T) {
	routes := []*Route{
		{
			Name:            "GetPanicOnMissingFFStructTag",
			Path:            "namespaces/{ns}/example1/test",
			Method:          http.MethodGet,
			Description:     "this is fine",
			JSONOutputValue: func() interface{} { return &TestNonTaggedType{} },
			JSONOutputCodes: []int{http.StatusOK},
		},
	}
	assert.PanicsWithValue(t, "invalid schema: FF00160: ffstruct tag is missing for 'noFFStructTag' on route 'GetPanicOnMissingFFStructTag'", func() {
		_ = NewSwaggerGen(&Options{
			Title:                     "UnitTest",
			Version:                   "1.0",
			BaseURL:                   "http://localhost:12345/api/v1",
			PanicOnMissingDescription: true,
		}).Generate(context.Background(), routes)
	})
}

func TestPanicOnMissingRouteDescription(t *testing.T) {
	routes := []*Route{
		{
			Name:            "GetPanicOnMissingRouteDescription",
			Path:            "namespaces/{ns}/example1/test",
			Method:          http.MethodGet,
			JSONOutputValue: func() interface{} { return &TestNonTaggedType{} },
			JSONOutputCodes: []int{http.StatusOK},
		},
	}
	assert.PanicsWithValue(t, "FF00159: API route description missing for route 'GetPanicOnMissingRouteDescription'", func() {
		_ = NewSwaggerGen(&Options{
			Title:                     "UnitTest",
			Version:                   "1.0",
			BaseURL:                   "http://localhost:12345/api/v1",
			PanicOnMissingDescription: true,
		}).Generate(context.Background(), routes)
	})
}

func TestPreTranslatedRouteDescription(t *testing.T) {
	routes := []*Route{
		{
			Name:                     "PostTagTest",
			Path:                     "namespaces/{ns}/example1/test",
			Method:                   http.MethodPost,
			JSONInputValue:           func() interface{} { return &TestInOutType{} },
			JSONOutputValue:          func() interface{} { return &TestInOutType{} },
			JSONOutputCodes:          []int{http.StatusOK},
			PreTranslatedDescription: "this is a description",
		},
	}
	swagger := NewSwaggerGen(&Options{
		Title:   "UnitTest",
		Version: "1.0",
		BaseURL: "http://localhost:12345/api/v1",
	}).Generate(context.Background(), routes)
	assert.NotNil(t, swagger.Paths["/namespaces/{ns}/example1/test"].Post.RequestBody.Value)
	description := swagger.Paths["/namespaces/{ns}/example1/test"].Post.Description
	assert.Equal(t, "this is a description", description)
}

func TestBaseURLVariables(t *testing.T) {
	doc := NewSwaggerGen(&Options{
		Title:   "UnitTest",
		Version: "1.0",
		BaseURL: "http://localhost:12345/api/v1/{param}",
		BaseURLVariables: map[string]BaseURLVariable{
			"param": {
				Default: "default-value",
			},
		},
	}).Generate(context.Background(), testRoutes)
	err := doc.Validate(context.Background())
	assert.NoError(t, err)

	server := doc.Servers[0]
	if assert.Contains(t, server.Variables, "param") {
		assert.Equal(t, "default-value", server.Variables["param"].Default)
	}

	b, err := yaml.Marshal(doc)
	assert.NoError(t, err)
	fmt.Print(string(b))
}
