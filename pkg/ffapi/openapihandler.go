// Copyright © 2024 Kaleido, Inc.
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
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/ghodss/yaml"
)

type OpenAPIFormat int

const (
	OpenAPIFormatJSON OpenAPIFormat = iota
	OpenAPIFormatYAML
)

type OpenAPIHandlerFactory struct {
	BaseSwaggerGenOptions   SwaggerGenOptions
	StaticPublicURL         string
	DynamicPublicURLHeader  string
	DynamicPublicURLBuilder func(req *http.Request) string
}

func SwaggerUIHTML(swaggerURL string) []byte {
	return []byte(fmt.Sprintf(
		`<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="UTF-8">
    <title>Swagger UI</title>
    <link rel="icon" type="image/png" href="/favicon-32x32.png" sizes="32x32" />
    <link rel="icon" type="image/png" href="/favicon-16x16.png" sizes="16x16" />
    <link rel="stylesheet" type="text/css" href="https://unpkg.com/swagger-ui-dist@5.29.0/swagger-ui.css">
		<link href="https://fonts.googleapis.com/css?family=Open+Sans:400,700|Source+Code+Pro:300,600|Titillium+Web:400,600,700" rel="stylesheet">
  </head>

  <body>
    <div id="swagger-ui"></div>

		<script src="https://unpkg.com/swagger-ui-dist@5.29.0/swagger-ui-standalone-preset.js" charset="UTF-8"></script>
		<script src="https://unpkg.com/swagger-ui-dist@5.29.0/swagger-ui-bundle.js" charset="UTF-8"></script>
    <script>
    window.onload = function() {
      // Begin Swagger UI call region
      const ui = SwaggerUIBundle({
        url: "%s",
        dom_id: '#swagger-ui',
        deepLinking: true,
        presets: [
          SwaggerUIBundle.presets.apis,
          SwaggerUIStandalonePreset
        ],
        plugins: [
          SwaggerUIBundle.plugins.DownloadUrl
        ],
        layout: "StandaloneLayout"
      });
      // End Swagger UI call region

      window.ui = ui;
    };
  </script>
  </body>
</html>
`,
		swaggerURL,
	))
}

func (ohf *OpenAPIHandlerFactory) getPublicURL(req *http.Request, relativePath string) string {
	publicURL := ""
	if ohf.DynamicPublicURLHeader != "" {
		publicURL = req.Header.Get(ohf.DynamicPublicURLHeader)
	} else if ohf.DynamicPublicURLBuilder != nil {
		publicURL = ohf.DynamicPublicURLBuilder(req)
	}
	if publicURL == "" {
		publicURL = ohf.StaticPublicURL
	}
	publicURL = strings.TrimSuffix(publicURL, "/")
	if len(relativePath) > 0 {
		publicURL = publicURL + "/" + strings.TrimPrefix(relativePath, "/")
	}
	return publicURL
}

func (ohf *OpenAPIHandlerFactory) OpenAPIHandler(apiPath string, format OpenAPIFormat, routes []*Route) HandlerFunction {
	return func(res http.ResponseWriter, req *http.Request) (int, error) {
		options := ohf.BaseSwaggerGenOptions

		// adding/overriding non-based options
		options.BaseURL = ohf.getPublicURL(req, apiPath)

		doc := NewSwaggerGen(&options).Generate(req.Context(), routes)
		if format == OpenAPIFormatJSON {
			res.Header().Add("Content-Type", "application/json")
			b, _ := json.Marshal(&doc)
			_, _ = res.Write(b)
		} else {
			res.Header().Add("Content-Type", "application/x-yaml")
			b, _ := yaml.Marshal(&doc)
			_, _ = res.Write(b)
		}
		return 200, nil
	}
}

func (ohf *OpenAPIHandlerFactory) OpenAPIHandlerVersioned(apiPath string, format OpenAPIFormat, apiVersionObject *APIVersion) HandlerFunction {
	return func(res http.ResponseWriter, req *http.Request) (int, error) {
		options := ohf.BaseSwaggerGenOptions

		// adding/overriding non-based options
		options.BaseURL = ohf.getPublicURL(req, apiPath)
		// version specific options must be explicitly provided
		options.Tags = apiVersionObject.Tags
		options.ExternalDocs = apiVersionObject.ExternalDocs

		doc := NewSwaggerGen(&options).Generate(req.Context(), apiVersionObject.Routes)
		if format == OpenAPIFormatJSON {
			res.Header().Add("Content-Type", "application/json")
			b, _ := json.Marshal(&doc)
			_, _ = res.Write(b)
		} else {
			res.Header().Add("Content-Type", "application/x-yaml")
			b, _ := yaml.Marshal(&doc)
			_, _ = res.Write(b)
		}
		return 200, nil
	}
}

func (ohf *OpenAPIHandlerFactory) SwaggerUIHandler(openAPIPath string) HandlerFunction {
	return func(res http.ResponseWriter, req *http.Request) (status int, err error) {
		res.Header().Add("Content-Type", "text/html")
		_, _ = res.Write(SwaggerUIHTML(ohf.getPublicURL(req, openAPIPath)))
		return 200, nil
	}
}
