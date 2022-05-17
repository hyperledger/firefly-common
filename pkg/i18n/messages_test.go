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

package i18n

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"golang.org/x/text/language"
)

var (
	TestError1  = FFE(language.AmericanEnglish, "FF99900", "Test error 1: %s")
	TestError2  = FFE(language.AmericanEnglish, "FF99901", "Test error 2: %s")
	TestError3  = FFE(language.AmericanEnglish, "FF99902", "Test error 3", 400)
	TestConfig1 = FFC(language.AmericanEnglish, "config.something.1", "Test config field 1", "some type")

	TestError1Lang2  = FFE(language.Spanish, "FF99900", "Error de prueba 1: %s")
	TestConfig1Lang2 = FFC(language.Spanish, "config.something.1", "campo de configuración de prueba", "some type")
)

func TestExpand(t *testing.T) {
	ctx := WithLang(context.Background(), language.AmericanEnglish)
	str := Expand(ctx, MessageKey(TestError1), "myinsert")
	assert.Equal(t, "Test error 1: myinsert", str)
}

func TestExpandNoLangContext(t *testing.T) {
	ctx := context.Background()
	str := Expand(ctx, MessageKey(TestError1), "myinsert")
	assert.Equal(t, "Test error 1: myinsert", str)
}

func TestExpandNoLangContextLang2(t *testing.T) {
	ctx := context.Background()
	SetLang("es")
	str := Expand(ctx, MessageKey(TestError1), "myinsert")
	assert.Equal(t, "Error de prueba 1: myinsert", str)
}

func TestExpandNoLangContextLang2Fallback(t *testing.T) {
	ctx := context.Background()
	SetLang("es")
	str := Expand(ctx, MessageKey(TestError2), "myinsert")
	assert.Equal(t, "Test error 2: myinsert", str)
}

func TestExpandLanguageFallback(t *testing.T) {
	ctx := WithLang(context.Background(), language.Spanish)
	str := Expand(ctx, MessageKey(TestError2), "myinsert")
	assert.Equal(t, "Test error 2: myinsert", str)
}

func TestExpandWithCode(t *testing.T) {
	ctx := WithLang(context.Background(), language.AmericanEnglish)
	str := ExpandWithCode(ctx, MessageKey(TestError2), "myinsert")
	assert.Equal(t, "FF99901: Test error 2: myinsert", str)
}

func TestExpandWithCodeLangaugeFallback(t *testing.T) {
	ctx := WithLang(context.Background(), language.Spanish)
	str := ExpandWithCode(ctx, MessageKey(TestError2), "myinsert")
	assert.Equal(t, "FF99901: Test error 2: myinsert", str)
}

func TestExpandWithCodeLang2(t *testing.T) {
	ctx := WithLang(context.Background(), language.Spanish)
	str := ExpandWithCode(ctx, MessageKey(TestError1), "myinsert")
	assert.Equal(t, "FF99900: Error de prueba 1: myinsert", str)
}

func TestGetStatusHint(t *testing.T) {
	code, ok := GetStatusHint(string(TestError3))
	assert.True(t, ok)
	assert.Equal(t, 400, code)
}

func TestDuplicateKey(t *testing.T) {
	FFM(language.AmericanEnglish, "FF109999", "test1")
	assert.Panics(t, func() {
		FFM(language.AmericanEnglish, "FF109999", "test2")
	})
}

func TestInvalidPrefixKey(t *testing.T) {
	assert.Panics(t, func() {
		FFE(language.AmericanEnglish, "ABCD1234", "test1")
	})
}

func TestConfigMessageKey(t *testing.T) {
	ctx := WithLang(context.Background(), language.AmericanEnglish)
	str := Expand(ctx, MessageKey(TestConfig1))
	assert.Equal(t, "Test config field 1", str)
}

func TestConfigMessageKeyLang2(t *testing.T) {
	ctx := WithLang(context.Background(), language.Spanish)
	str := Expand(ctx, MessageKey(TestConfig1))
	assert.Equal(t, "campo de configuración de prueba", str)
}

func TestGetFieldType(t *testing.T) {
	fieldType, ok := GetFieldType(string(TestConfig1))
	assert.True(t, ok)
	assert.Equal(t, "some type", fieldType)
}

func TestDuplicateConfigKey(t *testing.T) {
	FFC(language.AmericanEnglish, "config.test.2", "test2 description", "type")
	assert.Panics(t, func() {
		FFC(language.AmericanEnglish, "config.test.2", "test2 dupe", "dupe type")
	})
}
