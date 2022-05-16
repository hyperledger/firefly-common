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

import "golang.org/x/text/language"

var (
	registeredPrefixes = map[string]string{
		"FF00": "FireFly Common Utilities",
		"FF10": "FireFly Core",
		"FF21": "FireFly Transaction Manager",
		"FF22": "FireFly Signer",
		"FF99": "Test prefix",
	}
)

//revive:disable
var (
	MsgConfigFailed                = FFE(language.AmericanEnglish, "FF00101", "Failed to read config", 500)
	MsgBigIntTooLarge              = FFE(language.AmericanEnglish, "FF00103", "Byte length of serialized integer is too large %d (max=%d)")
	MsgBigIntParseFailed           = FFE(language.AmericanEnglish, "FF00104", "Failed to parse JSON value '%s' into BigInt")
	MsgTypeRestoreFailed           = FFE(language.AmericanEnglish, "FF00105", "Failed to restore type '%T' into '%T'")
	MsgInvalidHex                  = FFE(language.AmericanEnglish, "FF00106", "Invalid hex supplied", 400)
	MsgInvalidWrongLenB32          = FFE(language.AmericanEnglish, "FF00107", "Byte length must be 32 (64 hex characters)", 400)
	MsgUnknownValidatorType        = FFE(language.AmericanEnglish, "FF00108", "Unknown validator type: '%s'", 400)
	MsgDataValueIsNull             = FFE(language.AmericanEnglish, "FF00109", "Data value is null", 400)
	MsgBlobMismatchSealingData     = FFE(language.AmericanEnglish, "FF00110", "Blob mismatch when sealing data")
	MsgUnknownFieldValue           = FFE(language.AmericanEnglish, "FF00111", "Unknown %s '%v'", 400)
	MsgMissingRequiredField        = FFE(language.AmericanEnglish, "FF00112", "Field '%s' is required", 400)
	MsgDataInvalidHash             = FFE(language.AmericanEnglish, "FF00113", "Invalid data: hashes do not match Hash=%s Expected=%s", 400)
	MsgNilID                       = FFE(language.AmericanEnglish, "FF00114", "ID is nil")
	MsgGroupMustHaveMembers        = FFE(language.AmericanEnglish, "FF00115", "Group must have at least one member", 400)
	MsgEmptyMemberIdentity         = FFE(language.AmericanEnglish, "FF00116", "Identity is blank in member %d")
	MsgEmptyMemberNode             = FFE(language.AmericanEnglish, "FF00117", "Node is blank in member %d")
	MsgDuplicateMember             = FFE(language.AmericanEnglish, "FF00118", "Member %d is a duplicate org+node combination: %s", 400)
	MsgGroupInvalidHash            = FFE(language.AmericanEnglish, "FF00119", "Invalid group: hashes do not match Hash=%s Expected=%s", 400)
	MsgInvalidDIDForType           = FFE(language.AmericanEnglish, "FF00120", "Invalid FireFly DID '%s' for type='%s' namespace='%s' name='%s'", 400)
	MsgCustomIdentitySystemNS      = FFE(language.AmericanEnglish, "FF00121", "Custom identities cannot be defined in the '%s' namespace", 400)
	MsgSystemIdentityCustomNS      = FFE(language.AmericanEnglish, "FF00122", "System identities must be defined in the '%s' namespace", 400)
	MsgNilParentIdentity           = FFE(language.AmericanEnglish, "FF00124", "Identity of type '%s' must have a valid parent", 400)
	MsgNilOrNullObject             = FFE(language.AmericanEnglish, "FF00125", "Object is null")
	MsgUnknownIdentityType         = FFE(language.AmericanEnglish, "FF00126", "Unknown identity type: %s", 400)
	MsgJSONObjectParseFailed       = FFE(language.AmericanEnglish, "FF00127", "Failed to parse '%s' as JSON")
	MsgNilDataReferenceSealFail    = FFE(language.AmericanEnglish, "FF00128", "Invalid message: nil data reference at index %d", 400)
	MsgDupDataReferenceSealFail    = FFE(language.AmericanEnglish, "FF00129", "Invalid message: duplicate data reference at index %d", 400)
	MsgInvalidTXTypeForMessage     = FFE(language.AmericanEnglish, "FF00130", "Invalid transaction type for sending a message: %s", 400)
	MsgVerifyFailedNilHashes       = FFE(language.AmericanEnglish, "FF00131", "Invalid message: nil hashes", 400)
	MsgVerifyFailedInvalidHashes   = FFE(language.AmericanEnglish, "FF00132", "Invalid message: hashes do not match Hash=%s Expected=%s DataHash=%s DataHashExpected=%s", 400)
	MsgDuplicateArrayEntry         = FFE(language.AmericanEnglish, "FF00133", "Duplicate %s at index %d: '%s'", 400)
	MsgTooManyItems                = FFE(language.AmericanEnglish, "FF00134", "Maximum number of %s items is %d (supplied=%d)", 400)
	MsgFieldTooLong                = FFE(language.AmericanEnglish, "FF00135", "Field '%s' maximum length is %d", 400)
	MsgTimeParseFail               = FFE(language.AmericanEnglish, "FF00136", "Cannot parse time as RFC3339, Unix, or UnixNano: '%s'", 400)
	MsgDurationParseFail           = FFE(language.AmericanEnglish, "FF00137", "Unable to parse '%s' as duration string, or millisecond number", 400)
	MsgInvalidUUID                 = FFE(language.AmericanEnglish, "FF00138", "Invalid UUID supplied", 400)
	MsgSafeCharsOnly               = FFE(language.AmericanEnglish, "FF00139", "Field '%s' must include only alphanumerics (a-zA-Z0-9), dot (.), dash (-) and underscore (_)", 400)
	MsgInvalidName                 = FFE(language.AmericanEnglish, "FF00140", "Field '%s' must be 1-64 characters, including alphanumerics (a-zA-Z0-9), dot (.), dash (-) and underscore (_), and must start/end in an alphanumeric", 400)
	MsgNoUUID                      = FFE(language.AmericanEnglish, "FF00141", "Field '%s' must not be a UUID", 400)
	MsgInvalidFilterField          = FFE(language.AmericanEnglish, "FF00142", "Unknown filter '%s'", 400)
	MsgInvalidValueForFilterField  = FFE(language.AmericanEnglish, "FF00143", "Unable to parse value for filter '%s'", 400)
	MsgFieldMatchNoNull            = FFE(language.AmericanEnglish, "FF00144", "Comparison operator for field '%s' cannot accept a null value", 400)
	MsgFieldTypeNoStringMatching   = FFE(language.AmericanEnglish, "FF00145", "Field '%s' of type '%s' does not support partial or case-insensitive string matching", 400)
	MsgWSSendTimedOut              = FFE(language.AmericanEnglish, "FF00146", "Websocket send timed out")
	MsgWSClosing                   = FFE(language.AmericanEnglish, "FF00147", "Websocket closing")
	MsgWSConnectFailed             = FFE(language.AmericanEnglish, "FF00148", "Websocket connect failed")
	MsgInvalidURL                  = FFE(language.AmericanEnglish, "FF00149", "Invalid URL: '%s'")
	MsgWSHeartbeatTimeout          = FFE(language.AmericanEnglish, "FF00150", "Websocket heartbeat timed out after %.2fms", 500)
	MsgAPIServerStartFailed        = FFE(language.AmericanEnglish, "FF00151", "Unable to start listener on %s: %s")
	MsgInvalidCAFile               = FFE(language.AmericanEnglish, "FF00152", "Invalid CA certificates file")
	MsgTLSConfigFailed             = FFE(language.AmericanEnglish, "FF00153", "Failed to initialize TLS configuration")
	MsgContextCanceled             = FFE(language.AmericanEnglish, "FF00154", "Context canceled")
	MsgConnectorFailInvoke         = FFE(language.AmericanEnglish, "FF00155", "Connector request failed. requestId=%s failed to call connector API")
	MsgConnectorInvalidContentType = FFE(language.AmericanEnglish, "FF00156", "Connector request failed. requestId=%s invalid response content type: %s")
	MsgConnectorError              = FFE(language.AmericanEnglish, "FF00157", "Connector request failed. requestId=%s reason=%s error: %s")
)
