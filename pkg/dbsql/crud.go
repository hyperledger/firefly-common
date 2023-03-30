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
	"database/sql"
	"fmt"
	"reflect"
	"strings"

	sq "github.com/Masterminds/squirrel"
	"github.com/hyperledger/firefly-common/pkg/ffapi"
	"github.com/hyperledger/firefly-common/pkg/fftypes"
	"github.com/hyperledger/firefly-common/pkg/i18n"
	"github.com/hyperledger/firefly-common/pkg/log"
)

const (
	ColumnID      = "id"
	ColumnCreated = "created"
	ColumnUpdated = "updated"
)

type ChangeEventType int

const (
	Created ChangeEventType = iota
	Updated
	Deleted
)

type UpsertOptimization int

const (
	UpsertOptimizationSkip UpsertOptimization = iota
	UpsertOptimizationNew
	UpsertOptimizationExisting
)

type GetOption int

const (
	FailIfNotFound GetOption = iota
)

type PostCompletionHook func()

type Resource interface {
	GetID() *fftypes.UUID
	SetCreated(*fftypes.FFTime)
	SetUpdated(*fftypes.FFTime)
}

type ResourceBase struct {
	ID      *fftypes.UUID   `json:"id"`
	Created *fftypes.FFTime `json:"created"`
	Updated *fftypes.FFTime `json:"updated"`
}

func (r *ResourceBase) GetID() *fftypes.UUID {
	return r.ID
}

func (r *ResourceBase) SetCreated(t *fftypes.FFTime) {
	r.Created = t
}

func (r *ResourceBase) SetUpdated(t *fftypes.FFTime) {
	r.Updated = t
}

type CRUD[T Resource] interface {
	Validate()
	Upsert(ctx context.Context, inst T, optimization UpsertOptimization, hooks ...PostCompletionHook) (err error)
	InsertMany(ctx context.Context, instances []T, allowPartialSuccess bool, hooks ...PostCompletionHook) (err error)
	Insert(ctx context.Context, inst T, hooks ...PostCompletionHook) (err error)
	Replace(ctx context.Context, inst T, hooks ...PostCompletionHook) (err error)
	GetByID(ctx context.Context, id *fftypes.UUID, getOpts ...GetOption) (inst T, err error)
	GetMany(ctx context.Context, filter ffapi.Filter) (instances []T, fr *ffapi.FilterResult, err error)
	Update(ctx context.Context, id *fftypes.UUID, update ffapi.Update, hooks ...PostCompletionHook) (err error)
	UpdateSparse(ctx context.Context, sparseUpdate T, hooks ...PostCompletionHook) (err error)
	UpdateMany(ctx context.Context, filter ffapi.Filter, update ffapi.Update, hooks ...PostCompletionHook) (err error)
	Delete(ctx context.Context, id *fftypes.UUID, hooks ...PostCompletionHook) (err error)
}

type CrudBase[T Resource] struct {
	DB               *Database
	Table            string
	Columns          []string
	FilterFieldMap   map[string]string
	ImmutableColumns []string

	NilValue     func() T // nil value typed to T
	NewInstance  func() T
	ScopedFilter func() sq.Eq
	EventHandler func(id *fftypes.UUID, eventType ChangeEventType)
	GetFieldPtr  func(inst T, col string) interface{}

	// Optional extensions
	ReadTableAlias    string
	ReadOnlyColumns   []string
	ReadQueryModifier func(sq.SelectBuilder) sq.SelectBuilder
}

// Validate checks things that must be true about a CRUD collection using this framework.
// Intended for use in the unit tests of microservices (will exercise all the functions of the CrudBase):
// - the mandatory columns exist - id/created/updated
// - no column has the same name as the sequence column for the DB
// - a unique pointer is returned for each field column
// - the immutable columns exist
// - the other functions return valid data
func (c *CrudBase[T]) Validate() {
	inst := c.NewInstance()
	if isNil(inst) {
		panic("NewInstance() value must not be nil")
	}
	ptrs := map[string]interface{}{}
	fieldMap := map[string]bool{
		// Mandatory column checks
		ColumnID:      false,
		ColumnCreated: false,
		ColumnUpdated: false,
	}
	for _, col := range c.Columns {
		if ok, set := fieldMap[col]; ok && set {
			panic(fmt.Sprintf("%s is a duplicated column", col))
		}

		fieldMap[col] = true
		if col == c.DB.sequenceColumn {
			panic(fmt.Sprintf("cannot have column named '%s'", c.DB.sequenceColumn))
		}
		fieldPtr := c.GetFieldPtr(inst, col)
		ptrVal := reflect.ValueOf(fieldPtr)
		if ptrVal.Kind() != reflect.Ptr || !isNil(ptrVal.Elem().Interface()) {
			panic(fmt.Sprintf("field %s does not seem to be a pointer type - prevents null-check for PATCH semantics functioning", col))
		}
		ptrs[col] = fieldPtr
	}
	for col, set := range fieldMap {
		if !set {
			panic(fmt.Sprintf("mandatory column '%s' must be included in column list", col))
		}
	}
	if !isNil(c.NilValue()) {
		panic("NilValue() value must be nil")
	}
	if isNil(c.ScopedFilter()) {
		panic("ScopedFilter() value must not be nil")
	}
	if !isNil(c.GetFieldPtr(inst, fftypes.NewUUID().String())) {
		panic("GetFieldPtr() must return nil for unknown column")
	}
}

func (c *CrudBase[T]) idFilter(id *fftypes.UUID) sq.Eq {
	filter := c.ScopedFilter()
	if c.ReadTableAlias != "" {
		filter[fmt.Sprintf("%s.id", c.ReadTableAlias)] = id
	} else {
		filter["id"] = id
	}
	return filter
}

func (c *CrudBase[T]) buildUpdateList(ctx context.Context, update sq.UpdateBuilder, inst T, includeNil bool) sq.UpdateBuilder {
colLoop:
	for _, col := range c.Columns {
		for _, immutable := range append(c.ImmutableColumns, ColumnID, ColumnCreated, ColumnUpdated, c.DB.sequenceColumn) {
			if col == immutable {
				continue colLoop
			}
		}
		value := c.getFieldValue(inst, col)
		if includeNil || !isNil(value) {
			update = update.Set(col, value)
		}
	}
	update = update.Set(ColumnUpdated, fftypes.Now())
	return update
}

func (c *CrudBase[T]) updateFromInstance(ctx context.Context, tx *TXWrapper, inst T, includeNil bool) (int64, error) {
	update := sq.Update(c.Table)
	inst.SetUpdated(fftypes.Now())
	update = c.buildUpdateList(ctx, update, inst, includeNil)
	update = update.Where(c.idFilter(inst.GetID()))
	return c.DB.UpdateTx(ctx, c.Table, tx,
		update,
		func() {
			if c.EventHandler != nil {
				c.EventHandler(inst.GetID(), Updated)
			}
		})
}

func (c *CrudBase[T]) getFieldValue(inst T, col string) interface{} {
	// Validate() will have checked this is safe for microservices (as long as they use that at build time in their UTs)
	return reflect.ValueOf(c.GetFieldPtr(inst, col)).Elem().Interface()
}

func (c *CrudBase[T]) attemptInsert(ctx context.Context, tx *TXWrapper, inst T, requestConflictEmptyResult bool) (err error) {
	now := fftypes.Now()
	inst.SetCreated(now)
	inst.SetUpdated(now)
	insert := sq.Insert(c.Table).Columns(c.Columns...)
	values := make([]interface{}, len(c.Columns))
	for i, col := range c.Columns {
		values[i] = c.getFieldValue(inst, col)
	}
	insert = insert.Values(values...)
	_, err = c.DB.InsertTxExt(ctx, c.Table, tx, insert,
		func() {
			if c.EventHandler != nil {
				c.EventHandler(inst.GetID(), Created)
			}
		}, requestConflictEmptyResult)
	return err
}

func (c *CrudBase[T]) Upsert(ctx context.Context, inst T, optimization UpsertOptimization, hooks ...PostCompletionHook) (err error) {
	ctx, tx, autoCommit, err := c.DB.BeginOrUseTx(ctx)
	if err != nil {
		return err
	}
	defer c.DB.RollbackTx(ctx, tx, autoCommit)

	// First attempt the operation based on the optimization passed in.
	// The expectation is that the optimization will hit almost all of the time,
	// as only recovery paths require us to go down the un-optimized route.
	optimized := false
	if optimization == UpsertOptimizationNew {
		opErr := c.attemptInsert(ctx, tx, inst, true /* we want a failure here we can progress past */)
		optimized = opErr == nil
	} else if optimization == UpsertOptimizationExisting {
		rowsAffected, opErr := c.updateFromInstance(ctx, tx, inst, true /* full replace */)
		optimized = opErr == nil && rowsAffected == 1
	}

	if !optimized {
		// Do a select within the transaction to determine if the UUID already exists
		msgRows, _, err := c.DB.QueryTx(ctx, c.Table, tx,
			sq.Select(c.DB.sequenceColumn).
				From(c.Table).
				Where(c.idFilter(inst.GetID())),
		)
		if err != nil {
			return err
		}
		existing := msgRows.Next()
		msgRows.Close()

		if existing {
			// Replace the existing one
			if _, err = c.updateFromInstance(ctx, tx, inst, true /* full replace */); err != nil {
				return err
			}
		} else {
			// Get a useful error out of an insert attempt
			if err = c.attemptInsert(ctx, tx, inst, false); err != nil {
				return err
			}
		}
	}

	for _, hook := range hooks {
		tx.AddPostCommitHook(hook)
	}

	return c.DB.CommitTx(ctx, tx, autoCommit)
}

func (c *CrudBase[T]) InsertMany(ctx context.Context, instances []T, allowPartialSuccess bool, hooks ...PostCompletionHook) (err error) {

	ctx, tx, autoCommit, err := c.DB.BeginOrUseTx(ctx)
	if err != nil {
		return err
	}
	defer c.DB.RollbackTx(ctx, tx, autoCommit)
	if c.DB.Features().MultiRowInsert {
		insert := sq.Insert(c.Table).Columns(c.Columns...)
		for _, inst := range instances {
			values := make([]interface{}, len(c.Columns))
			for i, col := range c.Columns {
				values[i] = c.getFieldValue(inst, col)
			}
			insert = insert.Values(values...)
		}

		// Use a single multi-row insert
		sequences := make([]int64, len(instances))
		err := c.DB.InsertTxRows(ctx, c.Table, tx, insert, func() {
			for _, inst := range instances {
				if c.EventHandler != nil {
					c.EventHandler(inst.GetID(), Created)
				}
			}
		}, sequences, allowPartialSuccess)
		if err != nil {
			return err
		}
	} else {
		// Fall back to individual inserts grouped in a TX
		for _, inst := range instances {
			err := c.attemptInsert(ctx, tx, inst, false)
			if err != nil {
				return err
			}
		}
	}

	for _, hook := range hooks {
		tx.AddPostCommitHook(hook)
	}

	return c.DB.CommitTx(ctx, tx, autoCommit)

}

func (c *CrudBase[T]) Insert(ctx context.Context, inst T, hooks ...PostCompletionHook) (err error) {
	ctx, tx, autoCommit, err := c.DB.BeginOrUseTx(ctx)
	if err != nil {
		return err
	}
	defer c.DB.RollbackTx(ctx, tx, autoCommit)

	if err = c.attemptInsert(ctx, tx, inst, false); err != nil {
		return err
	}

	for _, hook := range hooks {
		tx.AddPostCommitHook(hook)
	}

	return c.DB.CommitTx(ctx, tx, autoCommit)
}

func (c *CrudBase[T]) Replace(ctx context.Context, inst T, hooks ...PostCompletionHook) (err error) {
	ctx, tx, autoCommit, err := c.DB.BeginOrUseTx(ctx)
	if err != nil {
		return err
	}
	defer c.DB.RollbackTx(ctx, tx, autoCommit)

	rowsAffected, err := c.updateFromInstance(ctx, tx, inst, true /* full replace */)
	if err != nil {
		return err
	} else if rowsAffected < 1 {
		return i18n.NewError(ctx, i18n.MsgDBNoRowsAffected)
	}

	for _, hook := range hooks {
		tx.AddPostCommitHook(hook)
	}

	return c.DB.CommitTx(ctx, tx, autoCommit)
}

func (c *CrudBase[T]) scanRow(ctx context.Context, cols []string, row *sql.Rows) (T, error) {
	inst := c.NewInstance()
	fieldPointers := make([]interface{}, len(cols))
	for i, col := range cols {
		fieldPointers[i] = c.GetFieldPtr(inst, col)
	}
	err := row.Scan(fieldPointers...)
	if err != nil {
		return c.NilValue(), i18n.WrapError(ctx, err, i18n.MsgDBReadErr, c.Table)
	}
	return inst, nil
}

func (c *CrudBase[T]) getReadCols() (tableFrom string, cols, readCols []string) {
	cols = append([]string{}, c.Columns...)
	if c.ReadOnlyColumns != nil {
		cols = append(cols, c.ReadOnlyColumns...)
	}
	tableFrom = c.Table
	readCols = cols
	if c.ReadTableAlias != "" {
		tableFrom = fmt.Sprintf("%s AS %s", c.Table, c.ReadTableAlias)
		readCols = make([]string, len(cols))
		for i, col := range cols {
			if strings.Contains(col, ".") {
				readCols[i] = cols[i]
			} else {
				readCols[i] = fmt.Sprintf("%s.%s", c.ReadTableAlias, col)
			}
		}
	}
	return tableFrom, cols, readCols
}

func (c *CrudBase[T]) GetByID(ctx context.Context, id *fftypes.UUID, getOpts ...GetOption) (inst T, err error) {

	failNotFound := false
	for _, o := range getOpts {
		switch o {
		case FailIfNotFound:
			failNotFound = true
		default:
			return c.NilValue(), i18n.NewError(ctx, i18n.MsgDBUnknownGetOption, o)
		}
	}

	tableFrom, cols, readCols := c.getReadCols()
	query := sq.Select(readCols...).
		From(tableFrom).
		Where(c.idFilter(id))
	if c.ReadQueryModifier != nil {
		query = c.ReadQueryModifier(query)
	}

	rows, _, err := c.DB.Query(ctx, c.Table, query)
	if err != nil {
		return c.NilValue(), err
	}
	defer rows.Close()

	if !rows.Next() {
		log.L(ctx).Debugf("%s '%s' not found", c.Table, id)
		if failNotFound {
			return c.NilValue(), i18n.NewError(ctx, i18n.Msg404NoResult)
		}
		return c.NilValue(), nil
	}

	inst, err = c.scanRow(ctx, cols, rows)
	if err != nil {
		return c.NilValue(), err
	}
	return inst, nil
}

func (c *CrudBase[T]) GetMany(ctx context.Context, filter ffapi.Filter) (instances []T, fr *ffapi.FilterResult, err error) {
	tableFrom, cols, readCols := c.getReadCols()
	query, fop, fi, err := c.DB.FilterSelect(ctx, c.ReadTableAlias, sq.Select(readCols...).From(tableFrom), filter, c.FilterFieldMap,
		[]interface{}{
			&ffapi.SortField{Field: c.DB.sequenceColumn, Descending: true},
		}, c.ScopedFilter())
	if err != nil {
		return nil, nil, err
	}
	if c.ReadQueryModifier != nil {
		query = c.ReadQueryModifier(query)
	}

	rows, tx, err := c.DB.Query(ctx, c.Table, query)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	instances = []T{}
	for rows.Next() {
		inst, err := c.scanRow(ctx, cols, rows)
		if err != nil {
			return nil, nil, err
		}
		instances = append(instances, inst)
	}
	return instances, c.DB.QueryRes(ctx, c.Table, tx, fop, fi), err
}

func (c *CrudBase[T]) Update(ctx context.Context, id *fftypes.UUID, update ffapi.Update, hooks ...PostCompletionHook) (err error) {
	return c.attemptUpdate(ctx, func(query sq.UpdateBuilder) (sq.UpdateBuilder, error) {
		return query.Where(sq.Eq{"id": id}), nil
	}, update, true, hooks...)
}

// Thanks to the testify/assert package for this
func isNil(o interface{}) bool {
	if o == nil {
		return true
	}

	containsKind := func(kinds []reflect.Kind, kind reflect.Kind) bool {
		for i := 0; i < len(kinds); i++ {
			if kind == kinds[i] {
				return true
			}
		}
		return false
	}

	value := reflect.ValueOf(o)
	kind := value.Kind()
	isNilableKind := containsKind(
		[]reflect.Kind{
			reflect.Chan, reflect.Func,
			reflect.Interface, reflect.Map,
			reflect.Ptr, reflect.Slice},
		kind)

	if isNilableKind && value.IsNil() {
		return true
	}

	return false
}

func (c *CrudBase[T]) UpdateSparse(ctx context.Context, sparseUpdate T, hooks ...PostCompletionHook) (err error) {
	ctx, tx, autoCommit, err := c.DB.BeginOrUseTx(ctx)
	if err != nil {
		return err
	}
	defer c.DB.RollbackTx(ctx, tx, autoCommit)

	updateCount, err := c.updateFromInstance(ctx, tx, sparseUpdate, false /* only non-nil fields */)
	if err != nil {
		return err
	}
	if updateCount < 1 {
		return i18n.NewError(ctx, i18n.MsgDBNoRowsAffected)
	}

	for _, hook := range hooks {
		tx.AddPostCommitHook(hook)
	}

	return c.DB.CommitTx(ctx, tx, autoCommit)
}

func (c *CrudBase[T]) UpdateMany(ctx context.Context, filter ffapi.Filter, update ffapi.Update, hooks ...PostCompletionHook) (err error) {
	return c.attemptUpdate(ctx, func(query sq.UpdateBuilder) (sq.UpdateBuilder, error) {
		return c.DB.FilterUpdate(ctx, query, filter, c.FilterFieldMap)
	}, update, false, hooks...)
}

func (c *CrudBase[T]) attemptUpdate(ctx context.Context, filterFn func(sq.UpdateBuilder) (sq.UpdateBuilder, error), update ffapi.Update, requireUpdateCount bool, hooks ...PostCompletionHook) (err error) {
	ctx, tx, autoCommit, err := c.DB.BeginOrUseTx(ctx)
	if err != nil {
		return err
	}
	defer c.DB.RollbackTx(ctx, tx, autoCommit)

	query, err := c.DB.BuildUpdate(sq.Update(c.Table).Where(c.ScopedFilter()), update, c.FilterFieldMap)
	if err == nil {
		query, err = filterFn(query)
	}
	query = query.Set(ColumnUpdated, fftypes.Now())
	if err != nil {
		return err
	}

	updateCount, err := c.DB.UpdateTx(ctx, c.Table, tx, query, nil /* no change events filter based update */)
	if err != nil {
		return err
	}

	if requireUpdateCount && updateCount < 1 {
		return i18n.NewError(ctx, i18n.MsgDBNoRowsAffected)
	}

	for _, hook := range hooks {
		tx.AddPostCommitHook(hook)
	}

	return c.DB.CommitTx(ctx, tx, autoCommit)
}

func (c *CrudBase[T]) Delete(ctx context.Context, id *fftypes.UUID, hooks ...PostCompletionHook) (err error) {

	ctx, tx, autoCommit, err := c.DB.BeginOrUseTx(ctx)
	if err != nil {
		return err
	}
	defer c.DB.RollbackTx(ctx, tx, autoCommit)

	err = c.DB.DeleteTx(ctx, c.Table, tx, sq.Delete(c.Table).Where(
		c.idFilter(id),
	), func() {
		if c.EventHandler != nil {
			c.EventHandler(id, Deleted)
		}
	})
	if err != nil {
		return err
	}

	for _, hook := range hooks {
		tx.AddPostCommitHook(hook)
	}

	return c.DB.CommitTx(ctx, tx, autoCommit)

}
