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
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/hyperledger/firefly-common/pkg/i18n"
)

var allMods = []string{"not", "caseInsensitive"}
var justCaseInsensitive = []string{"caseInsensitive"}

type FilterResultsWithCount struct {
	Count int64       `json:"count"`
	Total *int64      `json:"total,omitempty"` // omitted if a count was not calculated (AlwaysPaginate enabled, and count not specified)
	Items interface{} `json:"items"`
}

type filterModifiers struct {
	negate          bool
	caseInsensitive bool
	emptyIsNull     bool
	andCombine      bool
}

type FilterJSONBase struct {
	Not             bool   `ffstruct:"FilterJSON" json:"not,omitempty"`
	CaseInsensitive bool   `ffstruct:"FilterJSON" json:"caseInsensitive,omitempty"`
	Field           string `ffstruct:"FilterJSON" json:"field,omitempty"`
}

type FilterJSONKeyValue struct {
	FilterJSONBase
	Value SimpleFilterValue `ffstruct:"FilterJSON" json:"value,omitempty"`
}

type FilterJSONKeyValues struct {
	FilterJSONBase
	Values []SimpleFilterValue `ffstruct:"FilterJSON" json:"values,omitempty"`
}

type FilterJSON struct {
	Or                 []*FilterJSON          `ffstruct:"FilterJSON" json:"or,omitempty"`
	Equal              []*FilterJSONKeyValue  `ffstruct:"FilterJSON" json:"equal,omitempty"`
	Contains           []*FilterJSONKeyValue  `ffstruct:"FilterJSON" json:"contains,omitempty"`
	StartsWith         []*FilterJSONKeyValue  `ffstruct:"FilterJSON" json:"startsWith,omitempty"`
	LessThan           []*FilterJSONKeyValue  `ffstruct:"FilterJSON" json:"lessThan,omitempty"`
	LessThanOrEqual    []*FilterJSONKeyValue  `ffstruct:"FilterJSON" json:"lessThanOrEqual,omitempty"`
	GreaterThan        []*FilterJSONKeyValue  `ffstruct:"FilterJSON" json:"greaterThan,omitempty"`
	GreaterThanOrEqual []*FilterJSONKeyValue  `ffstruct:"FilterJSON" json:"greaterThanOrEqual,omitempty"`
	In                 []*FilterJSONKeyValues `ffstruct:"FilterJSON" json:"in,omitempty"`
}

type QueryJSON struct {
	FilterJSON
	Skip  *uint64  `ffstruct:"FilterJSON" json:"skip,omitempty"`
	Limit *uint64  `ffstruct:"FilterJSON" json:"limit,omitempty"`
	Sort  []string `ffstruct:"FilterJSON" json:"sort,omitempty"`
}

type SimpleFilterValue string

func (js *SimpleFilterValue) UnmarshalJSON(b []byte) error {
	var v interface{}
	err := json.Unmarshal(b, &v)
	if err != nil {
		return err
	}
	switch vi := v.(type) {
	case float64:
		*js = (SimpleFilterValue)(strconv.FormatFloat(vi, 'f', -1, 64))
		return nil
	case string:
		*js = (SimpleFilterValue)(vi)
		return nil
	case bool:
		*js = (SimpleFilterValue)(fmt.Sprintf("%t", vi))
		return nil
	default:
		return i18n.NewError(context.Background(), i18n.MsgJSONQueryValueUnsupported, string(b))
	}
}

func (js SimpleFilterValue) String() string {
	return (string)(js)
}

func (jq *QueryJSON) BuildFilter(ctx context.Context, qf QueryFactory) (Filter, error) {
	fb := qf.NewFilter(ctx)
	if jq.Skip != nil {
		fb = fb.Skip(*jq.Skip)
	}
	if jq.Limit != nil {
		fb = fb.Limit(*jq.Limit)
	}
	for _, s := range jq.Sort {
		fb = fb.Sort(s)
	}
	return jq.BuildSubFilter(ctx, fb, &jq.FilterJSON)
}

func (jq *QueryJSON) addSimpleFilters(fb FilterBuilder, jsonFilter *FilterJSON, andFilter AndFilter) AndFilter {
	for _, e := range jsonFilter.Equal {
		if e.CaseInsensitive {
			if e.Not {
				andFilter = andFilter.Condition(fb.NIeq(e.Field, e.Value.String()))
			} else {
				andFilter = andFilter.Condition(fb.IEq(e.Field, e.Value.String()))
			}
		} else {
			if e.Not {
				andFilter = andFilter.Condition(fb.Neq(e.Field, e.Value.String()))
			} else {
				andFilter = andFilter.Condition(fb.Eq(e.Field, e.Value.String()))
			}
		}
	}
	for _, e := range jsonFilter.Contains {
		if e.CaseInsensitive {
			if e.Not {
				andFilter = andFilter.Condition(fb.NotIContains(e.Field, e.Value.String()))
			} else {
				andFilter = andFilter.Condition(fb.IContains(e.Field, e.Value.String()))
			}
		} else {
			if e.Not {
				andFilter = andFilter.Condition(fb.NotContains(e.Field, e.Value.String()))
			} else {
				andFilter = andFilter.Condition(fb.Contains(e.Field, e.Value.String()))
			}
		}
	}
	for _, e := range jsonFilter.StartsWith {
		if e.CaseInsensitive {
			if e.Not {
				andFilter = andFilter.Condition(fb.NotIStartsWith(e.Field, e.Value.String()))
			} else {
				andFilter = andFilter.Condition(fb.IStartsWith(e.Field, e.Value.String()))
			}
		} else {
			if e.Not {
				andFilter = andFilter.Condition(fb.NotStartsWith(e.Field, e.Value.String()))
			} else {
				andFilter = andFilter.Condition(fb.StartsWith(e.Field, e.Value.String()))
			}
		}
	}
	return andFilter
}

func (jq *QueryJSON) BuildSubFilter(ctx context.Context, fb FilterBuilder, jsonFilter *FilterJSON) (Filter, error) {
	andFilter := jq.addSimpleFilters(fb, jsonFilter, fb.And())
	for _, e := range jsonFilter.LessThan {
		if e.CaseInsensitive || e.Not {
			return nil, i18n.NewError(ctx, i18n.MsgJSONQueryOpUnsupportedMod, "lessThan", allMods)
		}
		andFilter = andFilter.Condition(fb.Lt(e.Field, e.Value.String()))
	}
	for _, e := range jsonFilter.LessThanOrEqual {
		if e.CaseInsensitive || e.Not {
			return nil, i18n.NewError(ctx, i18n.MsgJSONQueryOpUnsupportedMod, "lessThanOrEqual", allMods)
		}
		andFilter = andFilter.Condition(fb.Lte(e.Field, e.Value.String()))
	}
	for _, e := range jsonFilter.GreaterThan {
		if e.CaseInsensitive || e.Not {
			return nil, i18n.NewError(ctx, i18n.MsgJSONQueryOpUnsupportedMod, "greaterThan", allMods)
		}
		andFilter = andFilter.Condition(fb.Gt(e.Field, e.Value.String()))
	}
	for _, e := range jsonFilter.GreaterThanOrEqual {
		if e.CaseInsensitive || e.Not {
			return nil, i18n.NewError(ctx, i18n.MsgJSONQueryOpUnsupportedMod, "greaterThanOrEqual", allMods)
		}
		andFilter = andFilter.Condition(fb.Gte(e.Field, e.Value.String()))
	}
	for _, e := range jsonFilter.In {
		if e.CaseInsensitive {
			return nil, i18n.NewError(ctx, i18n.MsgJSONQueryOpUnsupportedMod, "in", justCaseInsensitive)
		}
		if e.Not {
			andFilter = andFilter.Condition(fb.NotIn(e.Field, toDriverValues(e.Values)))
		} else {
			andFilter = andFilter.Condition(fb.In(e.Field, toDriverValues(e.Values)))
		}
	}
	if len(jsonFilter.Or) > 0 {
		childFilter := fb.Or()
		for _, child := range jsonFilter.Or {
			subFilter, err := jq.BuildSubFilter(ctx, fb, child)
			if err != nil {
				return nil, err
			}
			childFilter.Condition(subFilter)
		}
		if len(childFilter.Conditions()) == 1 {
			andFilter.Condition(childFilter.Conditions()[0])
		} else {
			andFilter.Condition(childFilter)
		}
	}
	if len(andFilter.Conditions()) == 1 {
		return andFilter.Conditions()[0], nil
	}
	return andFilter, nil
}

func toDriverValues(values []SimpleFilterValue) []driver.Value {
	driverValues := make([]driver.Value, len(values))
	for i, v := range values {
		driverValues[i] = v.String()
	}
	return driverValues
}
