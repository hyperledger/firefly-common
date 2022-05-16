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

package i18n

import (
	"context"
	"fmt"
	"strings"

	"golang.org/x/text/language"
	"golang.org/x/text/message"
	"golang.org/x/text/message/catalog"
)

// MessageKey is the lookup string for any translation
type MessageKey string

// ErrorMessageKey is a special lookup string conforming to FireFly's rules for error message registration
type ErrorMessageKey MessageKey

// ConfigMessageKey is a special lookup string conforming to FireFly's rules for configuration descriptions and types
type ConfigMessageKey MessageKey

// Expand for use in docs and logging - returns a translated message, translated the language of the context
// If a translation is not found for the language of the context, the default language text will be returned instead
func Expand(ctx context.Context, key MessageKey, inserts ...interface{}) string {
	translation := pFor(ctx).Sprintf(string(key), inserts...)
	if translation != string(key) {
		return translation
	}
	return defaultLangPrinter.Sprintf(string(key), inserts...)
}

// ExpandWithCode for use in error scenarios - returns a translated message with a "MSG012345:" prefix, translated the language of the context
func ExpandWithCode(ctx context.Context, key MessageKey, inserts ...interface{}) string {
	translation := string(key) + ": " + pFor(ctx).Sprintf(string(key), inserts...)
	if translation != string(key)+": "+string(key) {
		return translation
	}
	return string(key) + ": " + defaultLangPrinter.Sprintf(string(key), inserts...)
}

// WithLang sets the language on the context
func WithLang(ctx context.Context, lang language.Tag) context.Context {
	return context.WithValue(ctx, ctxLangKey{}, lang)
}

type (
	ctxLangKey struct{}
)

var statusHints = map[string]int{}
var fieldTypes = map[string]string{}
var msgIDUniq = map[string]bool{}

var defaultLangPrinter = message.NewPrinter(language.AmericanEnglish)

// FFE is the translations helper to register an error message
func FFE(language language.Tag, key, enTranslation string, statusHint ...int) ErrorMessageKey {
	validPrefix := false
	for rp := range registeredPrefixes {
		if strings.HasPrefix(key, rp) {
			validPrefix = true
			break
		}
	}
	if !validPrefix {
		panic(fmt.Sprintf("Message ID %s does not use one of the registered prefixes in the common utility package", key))
	}
	msgID := FFM(language, key, enTranslation)
	if len(statusHint) > 0 {
		statusHints[key] = statusHint[0]
	}
	return ErrorMessageKey(msgID)
}

// FFM is the enTranslations helper to define a new message (not used in translation files)
func FFM(language language.Tag, key, translation string) MessageKey {
	if checkKeyExists(language, key) {
		panic(fmt.Sprintf("Message ID %s re-used", key))
	}
	setKeyExists(language, key)
	_ = message.Set(language, key, catalog.String(translation))
	return MessageKey(key)
}

// FFC is the enTranslations helper to define a configuration key description and type
func FFC(language language.Tag, key, translation string, fieldType string) ConfigMessageKey {
	if checkKeyExists(language, key) {
		panic(fmt.Sprintf("Config ID %s re-used", key))
	}
	setKeyExists(language, key)
	fieldTypes[key] = fieldType
	_ = message.Set(language, key, catalog.String(translation))
	return ConfigMessageKey(key)
}

func pFor(ctx context.Context) *message.Printer {
	lang := ctx.Value(ctxLangKey{})
	if lang == nil {
		return defaultLangPrinter
	}
	return message.NewPrinter(lang.(language.Tag))
}

func init() {
	msgIDUniq = map[string]bool{} // Clear out that memory as no longer needed
}

func GetStatusHint(code string) (int, bool) {
	i, ok := statusHints[code]
	return i, ok
}

func GetFieldType(code string) (string, bool) {
	s, ok := fieldTypes[code]
	return s, ok
}

func setKeyExists(language language.Tag, key string) {
	msgIDUniq[fmt.Sprintf("%s_%s", language, key)] = true
	fmt.Println(msgIDUniq)
}

func checkKeyExists(language language.Tag, key string) bool {
	return msgIDUniq[fmt.Sprintf("%s_%s", language, key)]
}
