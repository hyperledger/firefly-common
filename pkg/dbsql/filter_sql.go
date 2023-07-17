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

package dbsql

import (
	"context"
	"fmt"
	"strings"

	sq "github.com/Masterminds/squirrel"
	"github.com/hyperledger/firefly-common/pkg/ffapi"
	"github.com/hyperledger/firefly-common/pkg/i18n"
)

func (s *Database) FilterSelect(ctx context.Context, tableName string, sel sq.SelectBuilder, filter ffapi.Filter, typeMap map[string]string, defaultSort []interface{}, preconditions ...sq.Sqlizer) (sq.SelectBuilder, sq.Sqlizer, *ffapi.FilterInfo, error) {
	fi, err := filter.Finalize()
	if err != nil {
		return sel, nil, nil, err
	}
	return s.filterSelectFinalized(ctx, tableName, sel, fi, typeMap, defaultSort, preconditions...)
}

func (s *Database) filterSelectFinalized(ctx context.Context, tableName string, sel sq.SelectBuilder, fi *ffapi.FilterInfo, typeMap map[string]string, defaultSort []interface{}, preconditions ...sq.Sqlizer) (sq.SelectBuilder, sq.Sqlizer, *ffapi.FilterInfo, error) {
	if len(fi.Sort) == 0 {
		for _, s := range defaultSort {
			switch v := s.(type) {
			case string:
				fi.Sort = append(fi.Sort, &ffapi.SortField{Field: v, Descending: true})
			case *ffapi.SortField:
				fi.Sort = append(fi.Sort, v)
			default:
				panic(fmt.Sprintf("unknown sort type: %v", v))
			}
		}
	}
	fop, err := s.refineQuery(ctx, tableName, fi, typeMap, preconditions...)
	if err != nil {
		return sel, nil, nil, err
	}

	sel = sel.Where(fop)

	if len(fi.GroupBy) > 0 {
		groupByWithResolvedFieldName := make([]string, len(fi.GroupBy))
		for i, gb := range fi.GroupBy {
			groupByWithResolvedFieldName[i] = s.mapField(tableName, gb, typeMap)
		}
		groupByString := strings.Join(groupByWithResolvedFieldName, ",")
		sel = sel.GroupBy(groupByString)
	}

	sort := make([]string, len(fi.Sort))
	var sortString string
	for i, sf := range fi.Sort {
		direction := ""
		if sf.Descending {
			direction = " DESC"
		}
		nulls := ""
		if sf.Nulls == ffapi.NullsFirst {
			nulls = " NULLS FIRST"
		} else if sf.Nulls == ffapi.NullsLast {
			nulls = " NULLS LAST"
		}
		sort[i] = fmt.Sprintf("%s%s%s", s.mapField(tableName, sf.Field, typeMap), direction, nulls)
	}
	sortString = strings.Join(sort, ", ")
	sel = sel.OrderBy(sortString)
	if fi.Skip > 0 {
		sel = sel.Offset(fi.Skip)
	}
	if fi.Limit > 0 {
		sel = sel.Limit(fi.Limit)
	}
	return sel, fop, fi, err
}

func (s *Database) refineQuery(ctx context.Context, tableName string, fi *ffapi.FilterInfo, tm map[string]string, preconditions ...sq.Sqlizer) (sq.Sqlizer, error) {
	fop, err := s.filterOp(ctx, tableName, fi, tm)
	if err != nil {
		return nil, err
	}
	if len(preconditions) > 0 {
		and := make(sq.And, len(preconditions)+1)
		copy(and, preconditions)
		and[len(preconditions)] = fop
		fop = and
	}
	return fop, nil
}

func (s *Database) BuildUpdate(sel sq.UpdateBuilder, update ffapi.Update, typeMap map[string]string) (sq.UpdateBuilder, error) {
	ui, err := update.Finalize()
	if err != nil {
		return sel, err
	}
	for _, so := range ui.SetOperations {

		sel = sel.Set(s.mapField("", so.Field, typeMap), so.Value)
	}
	return sel, nil
}

func (s *Database) FilterUpdate(ctx context.Context, update sq.UpdateBuilder, filter ffapi.Filter, typeMap map[string]string) (sq.UpdateBuilder, error) {
	fi, err := filter.Finalize()
	var fop sq.Sqlizer
	if err == nil {
		fop, err = s.filterOp(ctx, "", fi, typeMap)
	}
	if err != nil {
		return update, err
	}
	return update.Where(fop), nil
}

func (s *Database) escapeLike(value ffapi.FieldSerialization) string {
	v, _ := value.Value()
	vs, _ := v.(string)
	vs = strings.ReplaceAll(vs, "[", "[[]")
	vs = strings.ReplaceAll(vs, "%", "[%]")
	vs = strings.ReplaceAll(vs, "_", "[_]")
	return vs
}

func (s *Database) mapField(tableName, fieldName string, tm map[string]string) string {
	if fieldName == "sequence" {
		if tableName == "" {
			return s.sequenceColumn
		}
		return fmt.Sprintf("%s.seq", tableName)
	}
	var field = fieldName
	if tm != nil {
		if mf, ok := tm[fieldName]; ok {
			field = mf
		}
	}
	if tableName != "" {
		field = fmt.Sprintf("%s.%s", tableName, field)
	}
	return field
}

// newILike uses ILIKE if supported by DB, otherwise the "lower" approach
func (s *Database) newILike(field, value string) sq.Sqlizer {
	if s.features.UseILIKE {
		return sq.ILike{field: value}
	}
	return sq.Like{fmt.Sprintf("lower(%s)", field): strings.ToLower(value)}
}

// newNotILike uses ILIKE if supported by DB, otherwise the "lower" approach
func (s *Database) newNotILike(field, value string) sq.Sqlizer {
	if s.features.UseILIKE {
		return sq.NotILike{field: value}
	}
	return sq.NotLike{fmt.Sprintf("lower(%s)", field): strings.ToLower(value)}
}

func (s *Database) filterOp(ctx context.Context, tableName string, op *ffapi.FilterInfo, tm map[string]string) (sq.Sqlizer, error) {
	switch op.Op {
	case ffapi.FilterOpOr:
		return s.filterOr(ctx, tableName, op, tm)
	case ffapi.FilterOpAnd:
		return s.filterAnd(ctx, tableName, op, tm)
	case ffapi.FilterOpEq:
		return sq.Eq{s.mapField(tableName, op.Field, tm): op.Value}, nil
	case ffapi.FilterOpIEq:
		return s.newILike(s.mapField(tableName, op.Field, tm), s.escapeLike(op.Value)), nil
	case ffapi.FilterOpIn:
		return sq.Eq{s.mapField(tableName, op.Field, tm): op.Values}, nil
	case ffapi.FilterOpNeq:
		return sq.NotEq{s.mapField(tableName, op.Field, tm): op.Value}, nil
	case ffapi.FilterOpNIeq:
		return s.newNotILike(s.mapField(tableName, op.Field, tm), s.escapeLike(op.Value)), nil
	case ffapi.FilterOpNotIn:
		return sq.NotEq{s.mapField(tableName, op.Field, tm): op.Values}, nil
	case ffapi.FilterOpCont:
		return sq.Like{s.mapField(tableName, op.Field, tm): fmt.Sprintf("%%%s%%", s.escapeLike(op.Value))}, nil
	case ffapi.FilterOpNotCont:
		return sq.NotLike{s.mapField(tableName, op.Field, tm): fmt.Sprintf("%%%s%%", s.escapeLike(op.Value))}, nil
	case ffapi.FilterOpICont:
		return s.newILike(s.mapField(tableName, op.Field, tm), fmt.Sprintf("%%%s%%", s.escapeLike(op.Value))), nil
	case ffapi.FilterOpNotICont:
		return s.newNotILike(s.mapField(tableName, op.Field, tm), fmt.Sprintf("%s%%", s.escapeLike(op.Value))), nil
	case ffapi.FilterOpStartsWith:
		return sq.Like{s.mapField(tableName, op.Field, tm): fmt.Sprintf("%s%%", s.escapeLike(op.Value))}, nil
	case ffapi.FilterOpNotStartsWith:
		return sq.NotLike{s.mapField(tableName, op.Field, tm): fmt.Sprintf("%s%%", s.escapeLike(op.Value))}, nil
	case ffapi.FilterOpIStartsWith:
		return s.newILike(s.mapField(tableName, op.Field, tm), fmt.Sprintf("%s%%", s.escapeLike(op.Value))), nil
	case ffapi.FilterOpNotIStartsWith:
		return s.newNotILike(s.mapField(tableName, op.Field, tm), fmt.Sprintf("%s%%", s.escapeLike(op.Value))), nil
	case ffapi.FilterOpEndsWith:
		return sq.Like{s.mapField(tableName, op.Field, tm): fmt.Sprintf("%%%s", s.escapeLike(op.Value))}, nil
	case ffapi.FilterOpNotEndsWith:
		return sq.NotLike{s.mapField(tableName, op.Field, tm): fmt.Sprintf("%%%s", s.escapeLike(op.Value))}, nil
	case ffapi.FilterOpIEndsWith:
		return s.newILike(s.mapField(tableName, op.Field, tm), fmt.Sprintf("%%%s", s.escapeLike(op.Value))), nil
	case ffapi.FilterOpNotIEndsWith:
		return s.newNotILike(s.mapField(tableName, op.Field, tm), fmt.Sprintf("%%%s", s.escapeLike(op.Value))), nil
	case ffapi.FilterOpGt:
		return sq.Gt{s.mapField(tableName, op.Field, tm): op.Value}, nil
	case ffapi.FilterOpGte:
		return sq.GtOrEq{s.mapField(tableName, op.Field, tm): op.Value}, nil
	case ffapi.FilterOpLt:
		return sq.Lt{s.mapField(tableName, op.Field, tm): op.Value}, nil
	case ffapi.FilterOpLte:
		return sq.LtOrEq{s.mapField(tableName, op.Field, tm): op.Value}, nil
	default:
		return nil, i18n.NewError(ctx, i18n.MsgUnsupportedSQLOpInFilter, op.Op)
	}
}

func (s *Database) filterOr(ctx context.Context, tableName string, op *ffapi.FilterInfo, tm map[string]string) (sq.Sqlizer, error) {
	var err error
	or := make(sq.Or, len(op.Children))
	for i, c := range op.Children {
		if or[i], err = s.filterOp(ctx, tableName, c, tm); err != nil {
			return nil, err
		}
	}
	return or, nil
}

func (s *Database) filterAnd(ctx context.Context, tableName string, op *ffapi.FilterInfo, tm map[string]string) (sq.Sqlizer, error) {
	var err error
	and := make(sq.And, len(op.Children))
	for i, c := range op.Children {
		if and[i], err = s.filterOp(ctx, tableName, c, tm); err != nil {
			return nil, err
		}
	}
	return and, nil
}
