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
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBuildQueryJSONNestedAndOr(t *testing.T) {

	var qf QueryJSON
	err := json.Unmarshal([]byte(`{
		"skip": 5,
		"limit": 10,
		"sort": [
			"tag",
			"-sequence"
		],
		"equal": [
			{
				"field": "tag",
				"value": "a"
			}
		],
		"greaterThan": [
			{
				"field": "sequence",
				"value": 10
			}
		],
		"or": [
			{
				"equal": [
					{
						"field": "masked",
						"value": true
					}
				],
				"in": [
					{
						"field": "tag",
						"values": ["a","b","c"]
					}
				]
			},
			{
				"equal": [
					{
						"field": "masked",
						"value": false
					}
				]
			}
		]
	}`), &qf)
	assert.NoError(t, err)

	filter, err := qf.BuildFilter(context.Background(), TestQueryFactory)
	assert.NoError(t, err)

	fi, err := filter.Finalize()
	assert.NoError(t, err)

	assert.Equal(t, "( tag == 'a' ) && ( sequence >> 10 ) && ( ( ( masked == true ) && ( tag IN ['a','b','c'] ) ) || ( masked == false ) ) sort=tag,-sequence skip=5 limit=10", fi.String())
}

func TestBuildQuerySingleNestedOr(t *testing.T) {

	var qf QueryJSON
	err := json.Unmarshal([]byte(`{
		"or": [
			{
				"equal": [
					{
						"field": "tag",
						"value": "a"
					}
				]		
			}
		]
	}`), &qf)
	assert.NoError(t, err)

	filter, err := qf.BuildFilter(context.Background(), TestQueryFactory)
	assert.NoError(t, err)

	fi, err := filter.Finalize()
	assert.NoError(t, err)

	assert.Equal(t, "tag == 'a'", fi.String())
}

func TestBuildQueryJSONEqual(t *testing.T) {

	var qf QueryJSON
	err := json.Unmarshal([]byte(`{
		"skip": 5,
		"limit": 10,
		"count": true,
		"sort": [
			"tag",
			"sequence"
		],
		"equal": [
			{
				"field": "created",
				"value": 0
			},
			{
				"not": true,
				"field": "tag",
				"value": "abc"
			},
			{
				"caseInsensitive": true,
				"field": "tag",
				"value": "ABC"
			},
			{
				"caseInsensitive": true,
				"not": true,
				"field": "tag",
				"value": "abc"
			}
		]
	}`), &qf)
	assert.NoError(t, err)

	filter, err := qf.BuildFilter(context.Background(), TestQueryFactory)
	assert.NoError(t, err)

	fi, err := filter.Finalize()
	assert.NoError(t, err)

	assert.Equal(t, "( created == 0 ) && ( tag != 'abc' ) && ( tag := 'ABC' ) && ( tag ;= 'abc' ) sort=tag,sequence skip=5 limit=10 count=true", fi.String())
}

func TestBuildQueryJSONContains(t *testing.T) {

	var qf QueryJSON
	err := json.Unmarshal([]byte(`{
		"skip": 5,
		"limit": 10,
		"contains": [
			{
				"field": "tag",
				"value": 0
			},
			{
				"not": true,
				"field": "tag",
				"value": "abc"
			},
			{
				"caseInsensitive": true,
				"field": "tag",
				"value": "ABC"
			},
			{
				"caseInsensitive": true,
				"not": true,
				"field": "tag",
				"value": "abc"
			}
		]
	}`), &qf)
	assert.NoError(t, err)

	filter, err := qf.BuildFilter(context.Background(), TestQueryFactory)
	assert.NoError(t, err)

	fi, err := filter.Finalize()
	assert.NoError(t, err)

	assert.Equal(t, "( tag %= '0' ) && ( tag !% 'abc' ) && ( tag :% 'ABC' ) && ( tag ;% 'abc' ) skip=5 limit=10", fi.String())
}

func TestBuildQueryJSONStartsWith(t *testing.T) {

	var qf QueryJSON
	err := json.Unmarshal([]byte(`{
		"skip": 5,
		"limit": 10,
		"startsWith": [
			{
				"field": "tag",
				"value": 0
			},
			{
				"not": true,
				"field": "tag",
				"value": "abc"
			},
			{
				"caseInsensitive": true,
				"field": "tag",
				"value": "ABC"
			},
			{
				"caseInsensitive": true,
				"not": true,
				"field": "tag",
				"value": true
			}
		]
	}`), &qf)
	assert.NoError(t, err)

	filter, err := qf.BuildFilter(context.Background(), TestQueryFactory)
	assert.NoError(t, err)

	fi, err := filter.Finalize()
	assert.NoError(t, err)

	assert.Equal(t, "( tag ^= '0' ) && ( tag !^ 'abc' ) && ( tag :^ 'ABC' ) && ( tag ;^ 'true' ) skip=5 limit=10", fi.String())
}

func TestBuildQueryJSONGreaterThan(t *testing.T) {

	var qf QueryJSON
	err := json.Unmarshal([]byte(`{
		"skip": 5,
		"limit": 10,
		"greaterThan": [
			{
				"field": "sequence",
				"value": 0
			}
		]
	}`), &qf)
	assert.NoError(t, err)

	filter, err := qf.BuildFilter(context.Background(), TestQueryFactory)
	assert.NoError(t, err)

	fi, err := filter.Finalize()
	assert.NoError(t, err)

	assert.Equal(t, "sequence >> 0 skip=5 limit=10", fi.String())
}

func TestBuildQueryJSONLessThan(t *testing.T) {

	var qf QueryJSON
	err := json.Unmarshal([]byte(`{
		"skip": 5,
		"limit": 10,
		"lessThan": [
			{
				"field": "sequence",
				"value": "12345"
			}
		]
	}`), &qf)
	assert.NoError(t, err)

	filter, err := qf.BuildFilter(context.Background(), TestQueryFactory)
	assert.NoError(t, err)

	fi, err := filter.Finalize()
	assert.NoError(t, err)

	assert.Equal(t, "sequence << 12345 skip=5 limit=10", fi.String())
}

func TestBuildQueryJSONGreaterThanOrEqual(t *testing.T) {

	var qf QueryJSON
	err := json.Unmarshal([]byte(`{
		"skip": 5,
		"limit": 10,
		"greaterThanOrEqual": [
			{
				"field": "sequence",
				"value": 0
			}
		]
	}`), &qf)
	assert.NoError(t, err)

	filter, err := qf.BuildFilter(context.Background(), TestQueryFactory)
	assert.NoError(t, err)

	fi, err := filter.Finalize()
	assert.NoError(t, err)

	assert.Equal(t, "sequence >= 0 skip=5 limit=10", fi.String())
}

func TestBuildQueryJSONLessThanOrEqual(t *testing.T) {

	var qf QueryJSON
	err := json.Unmarshal([]byte(`{
		"skip": 5,
		"limit": 10,
		"lessThanOrEqual": [
			{
				"field": "sequence",
				"value": "12345"
			}
		]
	}`), &qf)
	assert.NoError(t, err)

	filter, err := qf.BuildFilter(context.Background(), TestQueryFactory)
	assert.NoError(t, err)

	fi, err := filter.Finalize()
	assert.NoError(t, err)

	assert.Equal(t, "sequence <= 12345 skip=5 limit=10", fi.String())
}

func TestBuildQueryJSONIn(t *testing.T) {

	var qf QueryJSON
	err := json.Unmarshal([]byte(`{
		"skip": 5,
		"limit": 10,
		"in": [
			{
				"field": "tag",
				"values": ["a","b","c"]
			},
			{
				"not": true,
				"field": "tag",
				"values": ["x","y","z"]
			}
		]
	}`), &qf)
	assert.NoError(t, err)

	filter, err := qf.BuildFilter(context.Background(), TestQueryFactory)
	assert.NoError(t, err)

	fi, err := filter.Finalize()
	assert.NoError(t, err)

	assert.Equal(t, "( tag IN ['a','b','c'] ) && ( tag NI ['x','y','z'] ) skip=5 limit=10", fi.String())
}

func TestBuildQueryJSONBadModifiers(t *testing.T) {

	var qf1 QueryJSON
	err := json.Unmarshal([]byte(`{"lessThan": [{"not": true, "field": "tag"}]}`), &qf1)
	assert.NoError(t, err)
	_, err = qf1.BuildFilter(context.Background(), TestQueryFactory)
	assert.Regexp(t, "FF00240", err)

	var qf2 QueryJSON
	err = json.Unmarshal([]byte(`{"lessThanOrEqual": [{"not": true, "field": "tag"}]}`), &qf2)
	assert.NoError(t, err)
	_, err = qf2.BuildFilter(context.Background(), TestQueryFactory)
	assert.Regexp(t, "FF00240", err)

	var qf3 QueryJSON
	err = json.Unmarshal([]byte(`{"greaterThan": [{"not": true, "field": "tag"}]}`), &qf3)
	assert.NoError(t, err)
	_, err = qf3.BuildFilter(context.Background(), TestQueryFactory)
	assert.Regexp(t, "FF00240", err)

	var qf4 QueryJSON
	err = json.Unmarshal([]byte(`{"greaterThanOrEqual": [{"not": true, "field": "tag"}]}`), &qf4)
	assert.NoError(t, err)
	_, err = qf4.BuildFilter(context.Background(), TestQueryFactory)
	assert.Regexp(t, "FF00240", err)

	var qf5 QueryJSON
	err = json.Unmarshal([]byte(`{"in": [{"caseInsensitive": true, "field": "tag"}]}`), &qf5)
	assert.NoError(t, err)
	_, err = qf5.BuildFilter(context.Background(), TestQueryFactory)
	assert.Regexp(t, "FF00240", err)

	var qf6 QueryJSON
	err = json.Unmarshal([]byte(`{"or": [{"in": [{"caseInsensitive": true, "field": "tag"}]}] }`), &qf6)
	assert.NoError(t, err)
	_, err = qf6.BuildFilter(context.Background(), TestQueryFactory)
	assert.Regexp(t, "FF00240", err)

}

func TestStringableParseFail(t *testing.T) {

	var js SimpleFilterValue
	err := js.UnmarshalJSON([]byte(`{!!! not json`))
	assert.Error(t, err)

	err = js.UnmarshalJSON([]byte(`{"this": "is an object"}`))
	assert.Error(t, err)

}

func TestBuildQueryJSONBadFields(t *testing.T) {

	var qf1 QueryJSON
	err := json.Unmarshal([]byte(`{"equal": [{"field": "wrong"}]}`), &qf1)
	assert.NoError(t, err)
	_, err = qf1.BuildFilter(context.Background(), TestQueryFactory)
	assert.Regexp(t, "FF00142", err)

	var qf2 QueryJSON
	err = json.Unmarshal([]byte(`{"contains": [{"field": "wrong"}]}`), &qf2)
	assert.NoError(t, err)
	_, err = qf2.BuildFilter(context.Background(), TestQueryFactory)
	assert.Regexp(t, "FF00142", err)

	var qf3 QueryJSON
	err = json.Unmarshal([]byte(`{"startsWith": [{"field": "wrong"}]}`), &qf3)
	assert.NoError(t, err)
	_, err = qf3.BuildFilter(context.Background(), TestQueryFactory)
	assert.Regexp(t, "FF00142", err)

	var qf4 QueryJSON
	err = json.Unmarshal([]byte(`{"lessThan": [{"field": "wrong"}]}`), &qf4)
	assert.NoError(t, err)
	_, err = qf4.BuildFilter(context.Background(), TestQueryFactory)
	assert.Regexp(t, "FF00142", err)

	var qf5 QueryJSON
	err = json.Unmarshal([]byte(`{"lessThanOrEqual": [{"field": "wrong"}]}`), &qf5)
	assert.NoError(t, err)
	_, err = qf5.BuildFilter(context.Background(), TestQueryFactory)
	assert.Regexp(t, "FF00142", err)

	var qf6 QueryJSON
	err = json.Unmarshal([]byte(`{"greaterThan": [{"field": "wrong"}]}`), &qf6)
	assert.NoError(t, err)
	_, err = qf6.BuildFilter(context.Background(), TestQueryFactory)
	assert.Regexp(t, "FF00142", err)

	var qf7 QueryJSON
	err = json.Unmarshal([]byte(`{"greaterThanOrEqual": [{"field": "wrong"}]}`), &qf7)
	assert.NoError(t, err)
	_, err = qf7.BuildFilter(context.Background(), TestQueryFactory)
	assert.Regexp(t, "FF00142", err)

	var qf8 QueryJSON
	err = json.Unmarshal([]byte(`{"in": [{"field": "wrong"}]}`), &qf8)
	assert.NoError(t, err)
	_, err = qf8.BuildFilter(context.Background(), TestQueryFactory)
	assert.Regexp(t, "FF00142", err)
}

func TestBuildQueryJSONDocumented(t *testing.T) {
	CheckObjectDocumented(&QueryJSON{})
}
