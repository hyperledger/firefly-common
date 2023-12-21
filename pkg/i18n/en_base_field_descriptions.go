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

package i18n

var (
	FilterJSONCaseInsensitive    = ffm("FilterJSON.caseInsensitive", "Configures whether the comparison is case sensitive - not supported for all operators")
	FilterJSONNot                = ffm("FilterJSON.not", "Negates the comparison operation, so 'equal' becomes 'not equal' for example - not supported for all operators")
	FilterJSONField              = ffm("FilterJSON.field", "Name of the field for the comparison operation")
	FilterJSONValue              = ffm("FilterJSON.value", "A JSON simple value to use in the comparison - must be a string, number or boolean and be parsable for the type of the filter field")
	FilterJSONValues             = ffm("FilterJSON.values", "Array of values to use in the comparison")
	FilterJSONContains           = ffm("FilterJSON.contains", "Array of field + value combinations to apply as string-contains filters - all filters must match")
	FilterJSONEqual              = ffm("FilterJSON.equal", "Array of field + value combinations to apply as equal filters - all must match")
	FilterJSONEq                 = ffm("FilterJSON.eq", "Shortname for equal")
	FilterJSONNEq                = ffm("FilterJSON.neq", "Shortcut for equal with all conditions negated (the not property of all children is overridden)")
	FilterJSONStartsWith         = ffm("FilterJSON.startsWith", "Array of field + value combinations to apply as starts-with filters - all filters must match")
	FilterJSONGreaterThan        = ffm("FilterJSON.greaterThan", "Array of field + value combinations to apply as greater-than filters - all filters must match")
	FilterJSONGT                 = ffm("FilterJSON.gt", "Short name for greaterThan")
	FilterJSONNGT                = ffm("FilterJSON.ngt", "Shortcut for greaterThan with all conditions negated (the not property of all children is overridden)")
	FilterJSONGreaterThanOrEqual = ffm("FilterJSON.greaterThanOrEqual", "Array of field + value combinations to apply as greater-than filters - all filters must match")
	FilterJSONGTE                = ffm("FilterJSON.gte", "Short name for greaterThanOrEqual")
	FilterJSONNGTE               = ffm("FilterJSON.ngte", "Shortcut for greaterThanOrEqual with all conditions negated (the not property of all children is overridden)")
	FilterJSONLessThan           = ffm("FilterJSON.lessThan", "Array of field + value combinations to apply as less-than-or-equal filters - all filters must match")
	FilterJSONLT                 = ffm("FilterJSON.lt", "Short name for lessThan")
	FilterJSONNLT                = ffm("FilterJSON.nlt", "Shortcut for lessThan with all conditions negated (the not property of all children is overridden)")
	FilterJSONLessThanOrEqual    = ffm("FilterJSON.lessThanOrEqual", "Array of field + value combinations to apply as less-than-or-equal filters - all filters must match")
	FilterJSONLTE                = ffm("FilterJSON.lte", "Short name for lessThanOrEqual")
	FilterJSONNLTE               = ffm("FilterJSON.nlte", "Shortcut for lessThanOrEqual with all conditions negated (the not property of all children is overridden)")
	FilterJSONIn                 = ffm("FilterJSON.in", "Array of field + values-array combinations to apply as 'in' filters (matching one of a set of values) - all filters must match")
	FilterJSONNIn                = ffm("FilterJSON.nin", "Shortcut for in with all conditions negated (the not property of all children is overridden)")
	FilterJSONLimit              = ffm("FilterJSON.limit", "Limit on the results to return")
	FilterJSONSkip               = ffm("FilterJSON.skip", "Number of results to skip before returning entries, for skip+limit based pagination")
	FilterJSONSort               = ffm("FilterJSON.sort", "Array of fields to sort by. A '-' prefix on a field requests that field is sorted in descending order")
	FilterJSONCount              = ffm("FilterJSON.count", "If true, the total number of entries that could be returned from the database will be calculated and returned as a 'total' (has a performance cost)")
	FilterJSONOr                 = ffm("FilterJSON.or", "Array of sub-queries where any sub-query can match to return results (OR combined). Note that within each sub-query all filters must match (AND combined)")
)
