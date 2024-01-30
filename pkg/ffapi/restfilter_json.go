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
	"context"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/hyperledger/firefly-common/pkg/i18n"
)

var allMods = []string{"not", "caseInsensitive"}
var justCaseInsensitive = []string{"caseInsensitive"}

// Note if ItemsResultTyped below might be preferred for new APIs (if you are able to adopt always-return {items:[]} style)
type FilterResultsWithCount struct {
	Count int64       `json:"count"`
	Total *int64      `json:"total,omitempty"` // omitted if a count was not calculated (AlwaysPaginate enabled, and count not specified)
	Items interface{} `json:"items"`
}

type ItemsResultTyped[T any] struct {
	Count int    `ffstruct:"CollectionResults" json:"count"`
	Total *int64 `ffstruct:"CollectionResults" json:"total,omitempty"` // omitted if a count was not calculated (AlwaysPaginate enabled, and count not specified)
	Items []T    `ffstruct:"CollectionResults" json:"items"`
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
	Eq                 []*FilterJSONKeyValue  `ffstruct:"FilterJSON" json:"eq,omitempty"`  // short name
	NEq                []*FilterJSONKeyValue  `ffstruct:"FilterJSON" json:"neq,omitempty"` // negated short name
	Contains           []*FilterJSONKeyValue  `ffstruct:"FilterJSON" json:"contains,omitempty"`
	StartsWith         []*FilterJSONKeyValue  `ffstruct:"FilterJSON" json:"startsWith,omitempty"`
	LessThan           []*FilterJSONKeyValue  `ffstruct:"FilterJSON" json:"lessThan,omitempty"`
	LT                 []*FilterJSONKeyValue  `ffstruct:"FilterJSON" json:"lt,omitempty"` // short name
	LessThanOrEqual    []*FilterJSONKeyValue  `ffstruct:"FilterJSON" json:"lessThanOrEqual,omitempty"`
	LTE                []*FilterJSONKeyValue  `ffstruct:"FilterJSON" json:"lte,omitempty"` // short name
	GreaterThan        []*FilterJSONKeyValue  `ffstruct:"FilterJSON" json:"greaterThan,omitempty"`
	GT                 []*FilterJSONKeyValue  `ffstruct:"FilterJSON" json:"gt,omitempty"` // short name
	GreaterThanOrEqual []*FilterJSONKeyValue  `ffstruct:"FilterJSON" json:"greaterThanOrEqual,omitempty"`
	GTE                []*FilterJSONKeyValue  `ffstruct:"FilterJSON" json:"gte,omitempty"` // short name
	In                 []*FilterJSONKeyValues `ffstruct:"FilterJSON" json:"in,omitempty"`
	NIn                []*FilterJSONKeyValues `ffstruct:"FilterJSON" json:"nin,omitempty"` // negated short name
}

type QueryJSON struct {
	FilterJSON
	Skip  *uint64  `ffstruct:"FilterJSON" json:"skip,omitempty"`
	Limit *uint64  `ffstruct:"FilterJSON" json:"limit,omitempty"`
	Sort  []string `ffstruct:"FilterJSON" json:"sort,omitempty"`
	Count *bool    `ffstruct:"FilterJSON" json:"count,omitempty"`
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
	if jq.Count != nil {
		fb = fb.Count(*jq.Count)
	}
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

func validateFilterField(ctx context.Context, fb FilterBuilder, fieldAnyCase string) (string, error) {
	for _, f := range fb.Fields() {
		if strings.EqualFold(fieldAnyCase, f) {
			return f, nil
		}
	}
	return "", i18n.NewError(ctx, i18n.MsgInvalidFilterField, fieldAnyCase)
}

func (jq *QueryJSON) addSimpleFilters(ctx context.Context, fb FilterBuilder, jsonFilter *FilterJSON, andFilter AndFilter) (AndFilter, error) {
	for _, e := range joinShortNames(jsonFilter.Equal, jsonFilter.Eq, jsonFilter.NEq) {
		field, err := validateFilterField(ctx, fb, e.Field)
		if err != nil {
			return nil, err
		}
		if e.CaseInsensitive {
			if e.Not {
				andFilter = andFilter.Condition(fb.NIeq(field, e.Value.String()))
			} else {
				andFilter = andFilter.Condition(fb.IEq(field, e.Value.String()))
			}
		} else {
			if e.Not {
				andFilter = andFilter.Condition(fb.Neq(field, e.Value.String()))
			} else {
				andFilter = andFilter.Condition(fb.Eq(field, e.Value.String()))
			}
		}
	}
	for _, e := range jsonFilter.Contains {
		field, err := validateFilterField(ctx, fb, e.Field)
		if err != nil {
			return nil, err
		}
		if e.CaseInsensitive {
			if e.Not {
				andFilter = andFilter.Condition(fb.NotIContains(field, e.Value.String()))
			} else {
				andFilter = andFilter.Condition(fb.IContains(field, e.Value.String()))
			}
		} else {
			if e.Not {
				andFilter = andFilter.Condition(fb.NotContains(field, e.Value.String()))
			} else {
				andFilter = andFilter.Condition(fb.Contains(field, e.Value.String()))
			}
		}
	}
	for _, e := range jsonFilter.StartsWith {
		field, err := validateFilterField(ctx, fb, e.Field)
		if err != nil {
			return nil, err
		}
		if e.CaseInsensitive {
			if e.Not {
				andFilter = andFilter.Condition(fb.NotIStartsWith(field, e.Value.String()))
			} else {
				andFilter = andFilter.Condition(fb.IStartsWith(field, e.Value.String()))
			}
		} else {
			if e.Not {
				andFilter = andFilter.Condition(fb.NotStartsWith(field, e.Value.String()))
			} else {
				andFilter = andFilter.Condition(fb.StartsWith(field, e.Value.String()))
			}
		}
	}
	return andFilter, nil
}

func joinShortNames(long, short, negated []*FilterJSONKeyValue) []*FilterJSONKeyValue {
	res := make([]*FilterJSONKeyValue, len(long)+len(short)+len(negated))
	copy(res, long)
	copy(res[len(long):], short)
	negs := res[len(short)+len(long):]
	copy(negs, negated)
	for _, n := range negs {
		n.Not = true
	}
	return res
}

func joinInAndNin(in, nin []*FilterJSONKeyValues) []*FilterJSONKeyValues {
	res := make([]*FilterJSONKeyValues, len(in)+len(nin))
	copy(res, in)
	negs := res[len(in):]
	copy(negs, nin)
	for _, n := range negs {
		n.Not = true
	}
	return res
}

func (jq *QueryJSON) BuildSubFilter(ctx context.Context, fb FilterBuilder, jsonFilter *FilterJSON) (Filter, error) {
	andFilter, err := jq.addSimpleFilters(ctx, fb, jsonFilter, fb.And())
	if err != nil {
		return nil, err
	}
	for _, e := range joinShortNames(jsonFilter.LessThan, jsonFilter.LT, nil) {
		field, err := validateFilterField(ctx, fb, e.Field)
		if err != nil {
			return nil, err
		}
		if e.CaseInsensitive || e.Not {
			return nil, i18n.NewError(ctx, i18n.MsgJSONQueryOpUnsupportedMod, "lessThan", allMods)
		}
		andFilter = andFilter.Condition(fb.Lt(field, e.Value.String()))
	}
	for _, e := range joinShortNames(jsonFilter.LessThanOrEqual, jsonFilter.LTE, nil) {
		field, err := validateFilterField(ctx, fb, e.Field)
		if err != nil {
			return nil, err
		}
		if e.CaseInsensitive || e.Not {
			return nil, i18n.NewError(ctx, i18n.MsgJSONQueryOpUnsupportedMod, "lessThanOrEqual", allMods)
		}
		andFilter = andFilter.Condition(fb.Lte(field, e.Value.String()))
	}
	for _, e := range joinShortNames(jsonFilter.GreaterThan, jsonFilter.GT, nil) {
		field, err := validateFilterField(ctx, fb, e.Field)
		if err != nil {
			return nil, err
		}
		if e.CaseInsensitive || e.Not {
			return nil, i18n.NewError(ctx, i18n.MsgJSONQueryOpUnsupportedMod, "greaterThan", allMods)
		}
		andFilter = andFilter.Condition(fb.Gt(field, e.Value.String()))
	}
	for _, e := range joinShortNames(jsonFilter.GreaterThanOrEqual, jsonFilter.GTE, nil) {
		field, err := validateFilterField(ctx, fb, e.Field)
		if err != nil {
			return nil, err
		}
		if e.CaseInsensitive || e.Not {
			return nil, i18n.NewError(ctx, i18n.MsgJSONQueryOpUnsupportedMod, "greaterThanOrEqual", allMods)
		}
		andFilter = andFilter.Condition(fb.Gte(field, e.Value.String()))
	}
	for _, e := range joinInAndNin(jsonFilter.In, jsonFilter.NIn) {
		field, err := validateFilterField(ctx, fb, e.Field)
		if err != nil {
			return nil, err
		}
		if e.CaseInsensitive {
			return nil, i18n.NewError(ctx, i18n.MsgJSONQueryOpUnsupportedMod, "in", justCaseInsensitive)
		}
		if e.Not {
			andFilter = andFilter.Condition(fb.NotIn(field, toDriverValues(e.Values)))
		} else {
			andFilter = andFilter.Condition(fb.In(field, toDriverValues(e.Values)))
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
		if len(childFilter.GetConditions()) == 1 {
			andFilter.Condition(childFilter.GetConditions()[0])
		} else {
			andFilter.Condition(childFilter)
		}
	}
	if len(andFilter.GetConditions()) == 1 {
		return andFilter.GetConditions()[0], nil
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
