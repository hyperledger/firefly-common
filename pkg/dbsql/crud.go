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
	"strings"

	sq "github.com/Masterminds/squirrel"
	"github.com/hyperledger/firefly-common/pkg/ffapi"
	"github.com/hyperledger/firefly-common/pkg/fftypes"
	"github.com/hyperledger/firefly-common/pkg/i18n"
	"github.com/hyperledger/firefly-common/pkg/log"
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

type PostCompletionHook func()

type WithID interface {
	GetID() *fftypes.UUID
}

type CRUD[T WithID] interface {
	Upsert(ctx context.Context, inst T, optimization UpsertOptimization, hooks ...PostCompletionHook) (err error)
	InsertMany(ctx context.Context, instances []T, allowPartialSuccess bool, hooks ...PostCompletionHook) (err error)
	Insert(ctx context.Context, inst T, hooks ...PostCompletionHook) (err error)
	Replace(ctx context.Context, inst T, hooks ...PostCompletionHook) (err error)
	GetByID(ctx context.Context, id *fftypes.UUID) (inst T, err error)
	GetMany(ctx context.Context, filter ffapi.Filter) (instances []T, fr *ffapi.FilterResult, err error)
	Update(ctx context.Context, id *fftypes.UUID, update ffapi.Update, hooks ...PostCompletionHook) (err error)
	UpdateMany(ctx context.Context, filter ffapi.Filter, update ffapi.Update, hooks ...PostCompletionHook) (err error)
	Delete(ctx context.Context, id *fftypes.UUID, hooks ...PostCompletionHook) (err error)
}

type CrudBase[T WithID] struct {
	DB             *Database
	Table          string
	Columns        []string
	FilterFieldMap map[string]string

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

func (c *CrudBase[T]) idFilter(id *fftypes.UUID) sq.Eq {
	filter := c.ScopedFilter()
	if c.ReadTableAlias != "" {
		filter[fmt.Sprintf("%s.id", c.ReadTableAlias)] = id
	} else {
		filter["id"] = id
	}
	return filter
}

func (c *CrudBase[T]) attemptReplace(ctx context.Context, tx *TXWrapper, inst T) (int64, error) {
	update := sq.Update(c.Table)
	for _, col := range c.Columns {
		update = update.Set(col, c.GetFieldPtr(inst, col))
	}
	update = update.Where(c.idFilter(inst.GetID()))
	return c.DB.UpdateTx(ctx, c.Table, tx,
		update,
		func() {
			if c.EventHandler != nil {
				c.EventHandler(inst.GetID(), Updated)
			}
		})
}

func (c *CrudBase[T]) attemptInsert(ctx context.Context, tx *TXWrapper, inst T, requestConflictEmptyResult bool) (err error) {
	insert := sq.Insert(c.Table).Columns(c.Columns...)
	values := make([]interface{}, len(c.Columns))
	for i, col := range c.Columns {
		values[i] = c.GetFieldPtr(inst, col)
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
		rowsAffected, opErr := c.attemptReplace(ctx, tx, inst)
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
			if _, err = c.attemptReplace(ctx, tx, inst); err != nil {
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
				values[i] = c.GetFieldPtr(inst, col)
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

	rowsAffected, err := c.attemptReplace(ctx, tx, inst)
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

func (c *CrudBase[T]) GetByID(ctx context.Context, id *fftypes.UUID) (inst T, err error) {
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
