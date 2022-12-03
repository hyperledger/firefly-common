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
		"FF23": "FireFly EVMConnect",
		"FF99": "Test prefix",
	}
)

var ffe = func(key, translation string, statusHint ...int) ErrorMessageKey {
	return FFE(language.AmericanEnglish, key, translation, statusHint...)
}

//revive:disable
var (
	MsgConfigFailed                = ffe("FF00101", "Failed to read config", 500)
	MsgBigIntTooLarge              = ffe("FF00103", "Byte length of serialized integer is too large %d (max=%d)")
	MsgBigIntParseFailed           = ffe("FF00104", "Failed to parse JSON value '%s' into BigInt")
	MsgTypeRestoreFailed           = ffe("FF00105", "Failed to restore type '%T' into '%T'")
	MsgInvalidHex                  = ffe("FF00106", "Invalid hex supplied", 400)
	MsgInvalidWrongLenB32          = ffe("FF00107", "Byte length must be 32 (64 hex characters)", 400)
	MsgUnknownValidatorType        = ffe("FF00108", "Unknown validator type: '%s'", 400)
	MsgDataValueIsNull             = ffe("FF00109", "Data value is null", 400)
	MsgBlobMismatchSealingData     = ffe("FF00110", "Blob mismatch when sealing data")
	MsgUnknownFieldValue           = ffe("FF00111", "Unknown %s '%v'", 400)
	MsgMissingRequiredField        = ffe("FF00112", "Field '%s' is required", 400)
	MsgDataInvalidHash             = ffe("FF00113", "Invalid data: hashes do not match Hash=%s Expected=%s", 400)
	MsgNilID                       = ffe("FF00114", "ID is nil")
	MsgGroupMustHaveMembers        = ffe("FF00115", "Group must have at least one member", 400)
	MsgEmptyMemberIdentity         = ffe("FF00116", "Identity is blank in member %d")
	MsgEmptyMemberNode             = ffe("FF00117", "Node is blank in member %d")
	MsgDuplicateMember             = ffe("FF00118", "Member %d is a duplicate org+node combination: %s", 400)
	MsgGroupInvalidHash            = ffe("FF00119", "Invalid group: hashes do not match Hash=%s Expected=%s", 400)
	MsgInvalidDIDForType           = ffe("FF00120", "Invalid FireFly DID '%s' for type='%s' namespace='%s' name='%s'", 400)
	MsgCustomIdentitySystemNS      = ffe("FF00121", "Custom identities cannot be defined in the '%s' namespace", 400)
	MsgSystemIdentityCustomNS      = ffe("FF00122", "System identities must be defined in the '%s' namespace", 400)
	MsgNilParentIdentity           = ffe("FF00124", "Identity of type '%s' must have a valid parent", 400)
	MsgNilOrNullObject             = ffe("FF00125", "Object is null")
	MsgUnknownIdentityType         = ffe("FF00126", "Unknown identity type: %s", 400)
	MsgJSONObjectParseFailed       = ffe("FF00127", "Failed to parse '%s' as JSON")
	MsgNilDataReferenceSealFail    = ffe("FF00128", "Invalid message: nil data reference at index %d", 400)
	MsgDupDataReferenceSealFail    = ffe("FF00129", "Invalid message: duplicate data reference at index %d", 400)
	MsgInvalidTXTypeForMessage     = ffe("FF00130", "Invalid transaction type for sending a message: %s", 400)
	MsgVerifyFailedNilHashes       = ffe("FF00131", "Invalid message: nil hashes", 400)
	MsgVerifyFailedInvalidHashes   = ffe("FF00132", "Invalid message: hashes do not match Hash=%s Expected=%s DataHash=%s DataHashExpected=%s", 400)
	MsgDuplicateArrayEntry         = ffe("FF00133", "Duplicate %s at index %d: '%s'", 400)
	MsgTooManyItems                = ffe("FF00134", "Maximum number of %s items is %d (supplied=%d)", 400)
	MsgFieldTooLong                = ffe("FF00135", "Field '%s' maximum length is %d", 400)
	MsgTimeParseFail               = ffe("FF00136", "Cannot parse time as RFC3339, Unix, or UnixNano: '%s'", 400)
	MsgDurationParseFail           = ffe("FF00137", "Unable to parse '%s' as duration string, or millisecond number", 400)
	MsgInvalidUUID                 = ffe("FF00138", "Invalid UUID supplied", 400)
	MsgSafeCharsOnly               = ffe("FF00139", "Field '%s' must include only alphanumerics (a-zA-Z0-9), dot (.), dash (-) and underscore (_)", 400)
	MsgInvalidName                 = ffe("FF00140", "Field '%s' must be 1-64 characters, including alphanumerics (a-zA-Z0-9), dot (.), dash (-) and underscore (_), and must start/end in an alphanumeric", 400)
	MsgNoUUID                      = ffe("FF00141", "Field '%s' must not be a UUID", 400)
	MsgInvalidFilterField          = ffe("FF00142", "Unknown filter '%s'", 400)
	MsgInvalidValueForFilterField  = ffe("FF00143", "Unable to parse value for filter '%s'", 400)
	MsgFieldMatchNoNull            = ffe("FF00144", "Comparison operator for field '%s' cannot accept a null value", 400)
	MsgFieldTypeNoStringMatching   = ffe("FF00145", "Field '%s' of type '%s' does not support partial or case-insensitive string matching", 400)
	MsgWSSendTimedOut              = ffe("FF00146", "Websocket send timed out")
	MsgWSClosing                   = ffe("FF00147", "Websocket closing")
	MsgWSConnectFailed             = ffe("FF00148", "Websocket connect failed")
	MsgInvalidURL                  = ffe("FF00149", "Invalid URL: '%s'")
	MsgWSHeartbeatTimeout          = ffe("FF00150", "Websocket heartbeat timed out after %.2fms", 500)
	MsgAPIServerStartFailed        = ffe("FF00151", "Unable to start listener on %s: %s")
	MsgInvalidCAFile               = ffe("FF00152", "Invalid CA certificates file")
	MsgTLSConfigFailed             = ffe("FF00153", "Failed to initialize TLS configuration")
	MsgContextCanceled             = ffe("FF00154", "Context canceled")
	MsgConnectorFailInvoke         = ffe("FF00155", "Connector request failed. requestId=%s failed to call connector API")
	MsgConnectorInvalidContentType = ffe("FF00156", "Connector request failed. requestId=%s invalid response content type: %s")
	MsgConnectorError              = ffe("FF00157", "Connector request failed. requestId=%s reason=%s error: %s")
	MsgFieldDescriptionMissing     = ffe("FF00158", "Field description missing for '%s' on route '%s'")
	MsgRouteDescriptionMissing     = ffe("FF00159", "API route description missing for route '%s'")
	MsgFFStructTagMissing          = ffe("FF00160", "ffstruct tag is missing for '%s' on route '%s'")
	MsgMultiPartFormReadError      = ffe("FF00161", "Error reading multi-part form input", 400)
	MsgInvalidContentType          = ffe("FF00162", "Invalid content type", 415)
	MsgFieldsAfterFile             = ffe("FF00163", "Additional form field sent after file in multi-part form (ignored): '%s'", 400)
	Msg404NoResult                 = ffe("FF00164", "No result found", 404)
	MsgResponseMarshalError        = ffe("FF00165", "Failed to serialize response data", 400)
	MsgRequestTimeout              = ffe("FF00166", "The request with id '%s' timed out after %.2fms", 408)
	Msg404NotFound                 = ffe("FF00167", "Not found", 404)
	MsgUnknownAuthPlugin           = ffe("FF00168", "Unknown auth plugin: '%s'")
	MsgUnauthorized                = ffe("FF00169", "Unauthorized", 401)
	MsgForbidden                   = ffe("FF00170", "Forbidden", 403)
	MsgInvalidEnum                 = ffe("FF00171", "'%s' is not a valid enum type", 400)
	MsgInvalidEnumValue            = ffe("FF00172", "'%s' is not a valid enum value for enum type '%s'. Valid options are: %v", 400)
	MsgDBInitFailed                = ffe("FF00173", "Database initialization failed")
	MsgDBQueryBuildFailed          = ffe("FF00174", "Database query builder failed")
	MsgDBBeginFailed               = ffe("FF00175", "Database begin transaction failed")
	MsgDBQueryFailed               = ffe("FF00176", "Database query failed")
	MsgDBInsertFailed              = ffe("FF00177", "Database insert failed")
	MsgDBUpdateFailed              = ffe("FF00178", "Database update failed")
	MsgDBDeleteFailed              = ffe("FF00179", "Database delete failed")
	MsgDBCommitFailed              = ffe("FF00180", "Database commit failed")
	MsgDBMissingJoin               = ffe("FF00181", "Database missing expected join entry in table '%s' for id '%s'")
	MsgDBReadErr                   = ffe("FF00182", "Database resultset read error from table '%s'")
	MsgMissingConfig               = ffe("FF00183", "Missing configuration '%s' for %s")
	MsgDBMigrationFailed           = ffe("FF00184", "Database migration failed")
	MsgDBNoSequence                = ffe("FF00185", "Failed to retrieve sequence for insert row %d (could mean duplicate insert)", 500)
	MsgDBMultiRowConfigError       = ffe("FF00186", "Database invalid configuration - using multi-row insert on DB plugin that does not support query syntax for input")
	MsgDBLockFailed                = ffe("FF00187", "Database lock failed")
	MsgHashMismatch                = ffe("FF00188", "Hash mismatch")
	MsgIDMismatch                  = ffe("FF00189", "ID mismatch")
	MsgUnsupportedSQLOpInFilter    = ffe("FF00190", "No SQL mapping implemented for filter operator '%s'", 400)
)
