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
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/Masterminds/squirrel"
	sq "github.com/Masterminds/squirrel"
	"github.com/hyperledger/firefly-common/pkg/ffapi"
	"github.com/hyperledger/firefly-common/pkg/fftypes"
	"github.com/hyperledger/firefly-common/pkg/log"
	"github.com/stretchr/testify/assert"
)

type TestCRUDable struct {
	ResourceBase
	NS     *string           `json:"namespace"`
	Field1 *string           `json:"f1"`
	Field2 *fftypes.FFBigInt `json:"f2"`
	Field3 *fftypes.JSONAny  `json:"f3"`
}

var CRUDableQueryFactory = &ffapi.QueryFields{
	"id":      &ffapi.UUIDField{},
	"created": &ffapi.TimeField{},
	"updated": &ffapi.TimeField{},
	"f1":      &ffapi.StringField{},
	"f2":      &ffapi.BigIntField{},
	"f3":      &ffapi.JSONField{},
}

type TestLinkable struct {
	ResourceBase
	NS          string           `json:"namespace"`
	CrudID      *fftypes.UUID    `json:"crudId"`
	Description string           `json:"description"`
	Field1      string           `json:"f1"`
	Field2      fftypes.FFBigInt `json:"f2"`
	Field3      *fftypes.JSONAny `json:"f3"`
}

var LinkableQueryFactory = &ffapi.QueryFields{
	"id":      &ffapi.UUIDField{},
	"created": &ffapi.TimeField{},
	"updated": &ffapi.TimeField{},
	"crud":    &ffapi.UUIDField{},
}

type TestCRUD struct {
	*CrudBase[*TestCRUDable]
	events      []ChangeEventType
	postCommit  func()
	postCommits int
}

func newCRUDCollection(db *Database, ns string) *TestCRUD {
	tc := &TestCRUD{
		CrudBase: &CrudBase[*TestCRUDable]{
			DB:    db,
			Table: "crudables",
			Columns: []string{
				ColumnID,
				ColumnCreated,
				ColumnUpdated,
				"ns",
				"field1",
				"field2",
				"field3",
			},
			FilterFieldMap: map[string]string{
				"f1": "field1",
				"f2": "field2",
				"f3": "field3",
			},
			NilValue:     func() *TestCRUDable { return nil },
			NewInstance:  func() *TestCRUDable { return &TestCRUDable{} },
			ScopedFilter: func() squirrel.Eq { return sq.Eq{"ns": ns} },
			EventHandler: nil, // set below
			GetFieldPtr: func(inst *TestCRUDable, col string) interface{} {
				switch col {
				case ColumnID:
					return &inst.ID
				case ColumnCreated:
					return &inst.Created
				case ColumnUpdated:
					return &inst.Updated
				case "ns":
					return &inst.NS
				case "field1":
					return &inst.Field1
				case "field2":
					return &inst.Field2
				case "field3":
					return &inst.Field3
				}
				return nil
			},
		},
	}
	tc.CrudBase.EventHandler = func(id string, eventType ChangeEventType) {
		tc.events = append(tc.events, eventType)
	}
	tc.postCommit = func() { tc.postCommits++ }
	return tc
}

func newLinkableCollection(db *Database, ns string) *CrudBase[*TestLinkable] {
	tc := &CrudBase[*TestLinkable]{
		DB:    db,
		Table: "linkables",
		Columns: []string{
			ColumnID,
			ColumnCreated,
			ColumnUpdated,
			"ns",
			"desc",
			"crud_id",
		},
		ReadTableAlias: "l",
		ReadOnlyColumns: []string{
			"c.field1",
			"c.field2",
			"c.field3",
		},
		FilterFieldMap: map[string]string{
			"description": "desc",
			"crud":        "crud_id",
		},
		ReadQueryModifier: func(query sq.SelectBuilder) sq.SelectBuilder {
			return query.LeftJoin("crudables AS c ON c.id = l.crud_id")
		},
		NilValue:     func() *TestLinkable { return nil },
		NewInstance:  func() *TestLinkable { return &TestLinkable{} },
		ScopedFilter: func() squirrel.Eq { return sq.Eq{"l.ns": ns} },
		EventHandler: nil, // set below
		GetFieldPtr: func(inst *TestLinkable, col string) interface{} {
			switch col {
			case ColumnID:
				return &inst.ID
			case ColumnCreated:
				return &inst.Created
			case ColumnUpdated:
				return &inst.Updated
			case "ns":
				return &inst.NS
			case "desc":
				return &inst.Description
			case "crud_id":
				return &inst.CrudID
			case "c.field1":
				return &inst.Field1
			case "c.field2":
				return &inst.Field2
			case "c.field3":
				return &inst.Field3
			}
			panic(fmt.Sprintf("unknown column: '%s'", col))
		},
	}
	return tc
}

func checkEqualExceptTimes(t *testing.T, o1, o2 TestCRUDable) {
	o1.Updated = nil
	o1.Created = nil
	j1, err := json.Marshal(o1)
	assert.NoError(t, err)
	o2.Updated = nil
	o2.Created = nil
	j2, err := json.Marshal(o2)
	assert.NoError(t, err)
	assert.JSONEq(t, string(j1), string(j2))
}

func strPtr(s string) *string {
	return &s
}

func TestCRUDWithDBEnd2End(t *testing.T) {
	log.SetLevel("trace")

	db, done := newSQLiteTestProvider(t)
	defer done()
	ctx := context.Background()

	c1 := &TestCRUDable{
		ResourceBase: ResourceBase{
			ID: fftypes.NewUUID(),
		},
		NS:     strPtr("ns1"),
		Field1: strPtr("hello1"),
		Field2: fftypes.NewFFBigInt(12345),
		Field3: fftypes.JSONAnyPtr(`{"some":"stuff"}`),
	}

	collection := newCRUDCollection(&db.Database, "ns1")
	var iCrud CRUD[*TestCRUDable] = collection.CrudBase
	iCrud.Validate()

	// Add a row
	err := iCrud.Insert(ctx, c1, collection.postCommit)
	assert.NoError(t, err)
	assert.Len(t, collection.events, 1)
	assert.Equal(t, Created, collection.events[0])
	collection.events = nil

	// Check we get it back
	c1copy, err := iCrud.GetByID(ctx, c1.ID.String())
	assert.NoError(t, err)
	checkEqualExceptTimes(t, *c1, *c1copy)

	// Upsert the existing row optimized
	c1copy.Field1 = strPtr("hello again - 1")
	created, err := iCrud.Upsert(ctx, c1copy, UpsertOptimizationExisting)
	assert.NoError(t, err)
	assert.False(t, created)
	c1copy1, err := iCrud.GetByID(ctx, c1.ID.String())
	assert.NoError(t, err)
	checkEqualExceptTimes(t, *c1copy, *c1copy1)
	assert.Len(t, collection.events, 1)
	assert.Equal(t, Updated, collection.events[0])
	collection.events = nil

	// Upsert the existing row un-optimized
	c1copy.Field1 = strPtr("hello again - 2")
	created, err = iCrud.Upsert(ctx, c1copy, UpsertOptimizationNew, collection.postCommit)
	assert.NoError(t, err)
	assert.False(t, created)
	c1copy2, err := iCrud.GetByID(ctx, c1.ID.String())
	assert.NoError(t, err)
	checkEqualExceptTimes(t, *c1copy, *c1copy2)

	// Explicitly replace it
	c1copy.Field1 = strPtr("hello again - 3")
	err = iCrud.Replace(ctx, c1copy, collection.postCommit)
	assert.NoError(t, err)
	c1copy3, err := iCrud.GetByID(ctx, c1.ID.String())
	assert.NoError(t, err)
	checkEqualExceptTimes(t, *c1copy, *c1copy3)

	// Explicitly update it
	c1copy.Field1 = strPtr("hello again - 4")
	err = iCrud.Update(ctx, c1copy.ID.String(), CRUDableQueryFactory.NewUpdate(ctx).Set(
		"f1", *c1copy.Field1,
	), collection.postCommit)
	assert.NoError(t, err)
	c1copy4, err := iCrud.GetByID(ctx, c1.ID.String())
	assert.NoError(t, err)
	checkEqualExceptTimes(t, *c1copy, *c1copy4)

	// Use simple PATCH semantics to updated it
	c1copy.Field1 = strPtr("hello again - 5")
	sparseUpdate := &TestCRUDable{
		ResourceBase: ResourceBase{
			ID: c1copy.ID,
		},
		Field1: strPtr("hello again - 5"),
	}
	err = iCrud.UpdateSparse(ctx, sparseUpdate, collection.postCommit)
	assert.NoError(t, err)
	c1copy5, err := iCrud.GetByID(ctx, c1.ID.String())
	assert.NoError(t, err)
	checkEqualExceptTimes(t, *c1copy, *c1copy5)

	// Cannot replace something that doesn't exist
	c2 := *c1
	c2.ID = fftypes.NewUUID()
	c2.Field1 = strPtr("bonjour")
	err = iCrud.Replace(ctx, &c2, collection.postCommit)
	assert.Regexp(t, "FF00205", err)

	// Optimized insert of another
	created, err = iCrud.Upsert(ctx, &c2, UpsertOptimizationNew)
	assert.NoError(t, err)
	assert.True(t, created)
	c2copy1, err := iCrud.GetByID(ctx, c2.ID.String())
	assert.NoError(t, err)
	checkEqualExceptTimes(t, c2, *c2copy1)

	// Check we can filter it with the new value
	cs, _, err := iCrud.GetMany(ctx, CRUDableQueryFactory.NewFilter(ctx).Eq(
		"f1", "bonjour",
	))
	assert.NoError(t, err)
	assert.Len(t, cs, 1)
	checkEqualExceptTimes(t, c2, *cs[0])

	// Insert a bunch in a batch
	bunchOfCRUDables := make([]*TestCRUDable, 10)
	for i := range bunchOfCRUDables {
		bunchOfCRUDables[i] = &TestCRUDable{
			ResourceBase: ResourceBase{
				ID: fftypes.NewUUID(),
			},
			NS:     strPtr("ns1"),
			Field1: strPtr(fmt.Sprintf("crudable[%.5d]", i)),
			Field2: fftypes.NewFFBigInt(919191),
		}
	}
	err = iCrud.InsertMany(ctx, bunchOfCRUDables, false, collection.postCommit)
	assert.NoError(t, err)

	// Grab one
	bunch5copy, err := iCrud.GetByID(ctx, bunchOfCRUDables[4].ID.String())
	assert.NoError(t, err)
	checkEqualExceptTimes(t, *bunchOfCRUDables[4], *bunch5copy)

	// Update them all
	err = iCrud.UpdateMany(ctx, CRUDableQueryFactory.NewFilter(ctx).Eq(
		"f2", "919191",
	), CRUDableQueryFactory.NewUpdate(ctx).Set(
		"f2", "929292",
	), collection.postCommit)
	assert.NoError(t, err)
	checkEqualExceptTimes(t, *bunchOfCRUDables[4], *bunch5copy)

	for i := range bunchOfCRUDables {
		ci, err := iCrud.GetByID(ctx, bunchOfCRUDables[i].ID.String())
		assert.NoError(t, err)
		assert.Equal(t, int64(929292), ci.Field2.Int64())
	}

	// Delete it
	err = iCrud.Delete(ctx, bunchOfCRUDables[4].ID.String(), collection.postCommit)
	assert.NoError(t, err)

	// Check it's gone
	goneOne, err := iCrud.GetByID(ctx, bunchOfCRUDables[4].ID.String())
	assert.NoError(t, err)
	assert.Nil(t, goneOne)

	// Check all the post commits above fired
	assert.Equal(t, 8, collection.postCommits)

}

func TestLeftJOINExample(t *testing.T) {
	log.SetLevel("trace")

	db, done := newSQLiteTestProvider(t)
	defer done()
	ctx := context.Background()

	crudables := newCRUDCollection(&db.Database, "ns1")
	linkables := newLinkableCollection(&db.Database, "ns1")

	c1 := &TestCRUDable{
		ResourceBase: ResourceBase{
			ID: fftypes.NewUUID(),
		},
		NS:     strPtr("ns1"),
		Field1: strPtr("linked1"),
		Field2: fftypes.NewFFBigInt(11111),
		Field3: fftypes.JSONAnyPtr(`{"linked":1}`),
	}
	l1 := &TestLinkable{
		ResourceBase: ResourceBase{
			ID: fftypes.NewUUID(),
		},
		NS:          "ns1",
		Description: "linked to C1",
		CrudID:      c1.ID,
	}

	err := crudables.Insert(ctx, c1)
	assert.NoError(t, err)

	err = linkables.Insert(ctx, l1)
	assert.NoError(t, err)

	l1Copy, err := linkables.GetByID(ctx, l1.ID.String())
	assert.NoError(t, err)
	assert.Equal(t, c1.ID, l1Copy.CrudID)
	assert.Equal(t, "linked to C1", l1Copy.Description)
	assert.Equal(t, "linked1", l1Copy.Field1 /* from JOIN */)
	assert.Equal(t, int64(11111), l1Copy.Field2.Int64() /* from JOIN */)
	assert.Equal(t, int64(1), l1Copy.Field3.JSONObject().GetInt64("linked") /* from JOIN */)

	l1s, _, err := linkables.GetMany(ctx, LinkableQueryFactory.NewFilter(ctx).Eq("crud", c1.ID.String()))
	assert.NoError(t, err)
	assert.Len(t, l1s, 1)
	assert.Equal(t, c1.ID, l1s[0].CrudID)
	assert.Equal(t, "linked to C1", l1s[0].Description)
	assert.Equal(t, "linked1", l1s[0].Field1 /* from JOIN */)
	assert.Equal(t, int64(11111), l1s[0].Field2.Int64() /* from JOIN */)
	assert.Equal(t, int64(1), l1s[0].Field3.JSONObject().GetInt64("linked") /* from JOIN */)
}

func TestUpsertFailBegin(t *testing.T) {
	db, mock := NewMockProvider().UTInit()
	tc := newCRUDCollection(&db.Database, "ns1")
	mock.ExpectBegin().WillReturnError(fmt.Errorf("pop"))
	_, err := tc.Upsert(context.Background(), &TestCRUDable{}, UpsertOptimizationSkip)
	assert.Regexp(t, "FF00175", err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestUpsertFailQuery(t *testing.T) {
	db, mock := NewMockProvider().UTInit()
	tc := newCRUDCollection(&db.Database, "ns1")
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT.*").WillReturnError(fmt.Errorf("pop"))
	_, err := tc.Upsert(context.Background(), &TestCRUDable{}, UpsertOptimizationSkip)
	assert.Regexp(t, "FF00176", err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestUpsertFailInsert(t *testing.T) {
	db, mock := NewMockProvider().UTInit()
	tc := newCRUDCollection(&db.Database, "ns1")
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT.*").WillReturnRows(mock.NewRows([]string{db.sequenceColumn}))
	mock.ExpectExec("INSERT.*").WillReturnError(fmt.Errorf("pop"))
	_, err := tc.Upsert(context.Background(), &TestCRUDable{}, UpsertOptimizationSkip)
	assert.Regexp(t, "FF00177", err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestUpsertFailUpdate(t *testing.T) {
	db, mock := NewMockProvider().UTInit()
	tc := newCRUDCollection(&db.Database, "ns1")
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT.*").WillReturnRows(mock.NewRows([]string{db.sequenceColumn}).AddRow(12345))
	mock.ExpectExec("UPDATE.*").WillReturnError(fmt.Errorf("pop"))
	_, err := tc.Upsert(context.Background(), &TestCRUDable{}, UpsertOptimizationSkip)
	assert.Regexp(t, "FF00178", err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestInsertManyBeginFail(t *testing.T) {
	db, mock := NewMockProvider().UTInit()
	tc := newCRUDCollection(&db.Database, "ns1")
	mock.ExpectBegin().WillReturnError(fmt.Errorf("pop"))
	err := tc.InsertMany(context.Background(), []*TestCRUDable{{}}, false)
	assert.Regexp(t, "FF00175", err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestInsertManyMultiRowOK(t *testing.T) {
	db, mock := NewMockProvider().UTInit()
	db.FakePSQLInsert = true
	db.features.MultiRowInsert = true
	tc := newCRUDCollection(&db.Database, "ns1")
	mock.ExpectBegin()
	mock.ExpectQuery(`INSERT INTO crudables.*VALUES \(.*\),\(.*\)`).WillReturnRows(
		sqlmock.NewRows([]string{db.sequenceColumn}).
			AddRow(123).
			AddRow(234),
	)
	mock.ExpectCommit()
	err := tc.InsertMany(context.Background(), []*TestCRUDable{
		{ResourceBase: ResourceBase{ID: fftypes.NewUUID()}},
		{ResourceBase: ResourceBase{ID: fftypes.NewUUID()}},
	}, false)
	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestInsertManyMultiRowFail(t *testing.T) {
	db, mock := NewMockProvider().UTInit()
	db.FakePSQLInsert = true
	db.features.MultiRowInsert = true
	tc := newCRUDCollection(&db.Database, "ns1")
	mock.ExpectBegin()
	mock.ExpectQuery(`INSERT INTO crudables.*VALUES \(.*\),\(.*\)`).WillReturnError(fmt.Errorf("pop"))
	err := tc.InsertMany(context.Background(), []*TestCRUDable{
		{ResourceBase: ResourceBase{ID: fftypes.NewUUID()}},
		{ResourceBase: ResourceBase{ID: fftypes.NewUUID()}},
	}, false)
	assert.Regexp(t, "FF00177", err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestInsertManyFallbackSingleRowFail(t *testing.T) {
	db, mock := NewMockProvider().UTInit()
	tc := newCRUDCollection(&db.Database, "ns1")
	mock.ExpectBegin()
	mock.ExpectExec(`INSERT INTO crudables.*`).WillReturnError(fmt.Errorf("pop"))
	err := tc.InsertMany(context.Background(), []*TestCRUDable{
		{ResourceBase: ResourceBase{ID: fftypes.NewUUID()}},
		{ResourceBase: ResourceBase{ID: fftypes.NewUUID()}},
	}, false)
	assert.Regexp(t, "FF00177", err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestInsertBeginFail(t *testing.T) {
	db, mock := NewMockProvider().UTInit()
	tc := newCRUDCollection(&db.Database, "ns1")
	mock.ExpectBegin().WillReturnError(fmt.Errorf("pop"))
	err := tc.Insert(context.Background(), &TestCRUDable{})
	assert.Regexp(t, "FF00175", err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestInsertInsertFail(t *testing.T) {
	db, mock := NewMockProvider().UTInit()
	tc := newCRUDCollection(&db.Database, "ns1")
	mock.ExpectBegin()
	mock.ExpectExec("INSERT.*").WillReturnError(fmt.Errorf("pop"))
	err := tc.Insert(context.Background(), &TestCRUDable{})
	assert.Regexp(t, "FF00177", err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestReplaceBeginFail(t *testing.T) {
	db, mock := NewMockProvider().UTInit()
	tc := newCRUDCollection(&db.Database, "ns1")
	mock.ExpectBegin().WillReturnError(fmt.Errorf("pop"))
	err := tc.Replace(context.Background(), &TestCRUDable{})
	assert.Regexp(t, "FF00175", err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestReplaceUpdateFail(t *testing.T) {
	db, mock := NewMockProvider().UTInit()
	tc := newCRUDCollection(&db.Database, "ns1")
	mock.ExpectBegin()
	mock.ExpectExec("UPDATE.*").WillReturnError(fmt.Errorf("pop"))
	err := tc.Replace(context.Background(), &TestCRUDable{})
	assert.Regexp(t, "FF00178", err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestGetByIDNotFound(t *testing.T) {
	db, mock := NewMockProvider().UTInit()
	tc := newCRUDCollection(&db.Database, "ns1")
	mock.ExpectQuery("SELECT.*").WillReturnRows(sqlmock.NewRows([]string{}))
	_, err := tc.GetByID(context.Background(), fftypes.NewUUID().String(), FailIfNotFound)
	assert.Regexp(t, "FF00164", err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestGetByIDBadOpts(t *testing.T) {
	db, _ := NewMockProvider().UTInit()
	tc := newCRUDCollection(&db.Database, "ns1")
	_, err := tc.GetByID(context.Background(), fftypes.NewUUID().String(), GetOption(999))
	assert.Regexp(t, "FF00212", err)
}

func TestGetByIDSelectFail(t *testing.T) {
	db, mock := NewMockProvider().UTInit()
	tc := newCRUDCollection(&db.Database, "ns1")
	mock.ExpectQuery("SELECT.*").WillReturnError(fmt.Errorf("pop"))
	_, err := tc.GetByID(context.Background(), fftypes.NewUUID().String())
	assert.Regexp(t, "FF00176", err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestGetByIDScanFail(t *testing.T) {
	db, mock := NewMockProvider().UTInit()
	tc := newCRUDCollection(&db.Database, "ns1")
	mock.ExpectQuery("SELECT.*").WillReturnRows(sqlmock.NewRows([]string{}).AddRow())
	_, err := tc.GetByID(context.Background(), fftypes.NewUUID().String())
	assert.Regexp(t, "FF00182", err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestGetByManySelectFail(t *testing.T) {
	db, mock := NewMockProvider().UTInit()
	tc := newCRUDCollection(&db.Database, "ns1")
	mock.ExpectQuery("SELECT.*").WillReturnError(fmt.Errorf("pop"))
	_, _, err := tc.GetMany(context.Background(), CRUDableQueryFactory.NewFilter(context.Background()).And())
	assert.Regexp(t, "FF00176", err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestGetByManyScanFail(t *testing.T) {
	db, mock := NewMockProvider().UTInit()
	tc := newCRUDCollection(&db.Database, "ns1")
	mock.ExpectQuery("SELECT.*").WillReturnRows(sqlmock.NewRows([]string{}).AddRow())
	_, _, err := tc.GetMany(context.Background(), CRUDableQueryFactory.NewFilter(context.Background()).And())
	assert.Regexp(t, "FF00182", err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestGetByManyFilterFail(t *testing.T) {
	db, mock := NewMockProvider().UTInit()
	tc := newCRUDCollection(&db.Database, "ns1")
	_, _, err := tc.GetMany(context.Background(), CRUDableQueryFactory.NewFilter(context.Background()).Eq("wrong", 123))
	assert.Regexp(t, "FF00142", err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestUpdateBeginFail(t *testing.T) {
	db, mock := NewMockProvider().UTInit()
	tc := newCRUDCollection(&db.Database, "ns1")
	mock.ExpectBegin().WillReturnError(fmt.Errorf("pop"))
	mock.ExpectExec("UPDATE.*").WillReturnResult(driver.ResultNoRows)
	err := tc.Update(context.Background(), fftypes.NewUUID().String(), CRUDableQueryFactory.NewUpdate(context.Background()).Set("f1", "12345"))
	assert.Regexp(t, "FF00175", err)
}

func TestUpdateBuildUpdateFail(t *testing.T) {
	db, mock := NewMockProvider().UTInit()
	tc := newCRUDCollection(&db.Database, "ns1")
	mock.ExpectBegin()
	err := tc.UpdateMany(context.Background(),
		CRUDableQueryFactory.NewFilter(context.Background()).Eq("wrong", 123),
		CRUDableQueryFactory.NewUpdate(context.Background()).Set("f1", "12345"))
	assert.Regexp(t, "FF00142", err)
}

func TestUpdateUpdateFail(t *testing.T) {
	db, mock := NewMockProvider().UTInit()
	tc := newCRUDCollection(&db.Database, "ns1")
	mock.ExpectBegin()
	mock.ExpectExec("UPDATE.*").WillReturnError(fmt.Errorf("pop"))
	err := tc.Update(context.Background(), fftypes.NewUUID().String(), CRUDableQueryFactory.NewUpdate(context.Background()).Set("f1", "12345"))
	assert.Regexp(t, "FF00178", err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestUpdateNowRows(t *testing.T) {
	db, mock := NewMockProvider().UTInit()
	tc := newCRUDCollection(&db.Database, "ns1")
	mock.ExpectBegin()
	mock.ExpectExec("UPDATE.*").WillReturnResult(driver.ResultNoRows)
	err := tc.Update(context.Background(), fftypes.NewUUID().String(), CRUDableQueryFactory.NewUpdate(context.Background()).Set("f1", "12345"))
	assert.Regexp(t, "FF00205", err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestDeleteBeginFail(t *testing.T) {
	db, mock := NewMockProvider().UTInit()
	tc := newCRUDCollection(&db.Database, "ns1")
	mock.ExpectBegin().WillReturnError(fmt.Errorf("pop"))
	mock.ExpectExec("UPDATE.*").WillReturnResult(driver.ResultNoRows)
	err := tc.Delete(context.Background(), fftypes.NewUUID().String())
	assert.Regexp(t, "FF00175", err)
}

func TestDeleteDeleteFail(t *testing.T) {
	db, mock := NewMockProvider().UTInit()
	tc := newCRUDCollection(&db.Database, "ns1")
	mock.ExpectBegin()
	mock.ExpectExec("DELETE.*").WillReturnError(fmt.Errorf("pop"))
	err := tc.Delete(context.Background(), fftypes.NewUUID().String())
	assert.Regexp(t, "FF00179", err)
}

func TestIsNilNonPtrType(t *testing.T) {
	assert.False(t, isNil(0))
}

func TestUpdateSparseFailBegin(t *testing.T) {
	db, mock := NewMockProvider().UTInit()
	tc := newCRUDCollection(&db.Database, "ns1")
	mock.ExpectBegin().WillReturnError(fmt.Errorf("pop"))
	err := tc.UpdateSparse(context.Background(), &TestCRUDable{})
	assert.Regexp(t, "FF00175", err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestUpdateSparseFailUpdate(t *testing.T) {
	db, mock := NewMockProvider().UTInit()
	tc := newCRUDCollection(&db.Database, "ns1")
	mock.ExpectBegin()
	mock.ExpectExec("UPDATE.*").WillReturnError(fmt.Errorf("pop"))
	err := tc.UpdateSparse(context.Background(), &TestCRUDable{})
	assert.Regexp(t, "FF00178", err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestUpdateSparseFailNoResult(t *testing.T) {
	db, mock := NewMockProvider().UTInit()
	tc := newCRUDCollection(&db.Database, "ns1")
	mock.ExpectBegin()
	mock.ExpectExec("UPDATE.*").WillReturnResult(sqlmock.NewResult(0, 0))
	err := tc.UpdateSparse(context.Background(), &TestCRUDable{})
	assert.Regexp(t, "FF00205", err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestValidateNewInstanceNil(t *testing.T) {
	tc := &CrudBase[*TestCRUDable]{
		NewInstance: func() *TestCRUDable { return nil },
	}
	assert.PanicsWithValue(t, "NewInstance() value must not be nil", func() {
		tc.Validate()
	})
}

func TestValidateDupSeqColumn(t *testing.T) {
	db, _ := NewMockProvider().UTInit()
	tc := &CrudBase[*TestCRUDable]{
		DB:          &db.Database,
		NewInstance: func() *TestCRUDable { return &TestCRUDable{} },
		Columns:     []string{"seq"},
	}
	assert.PanicsWithValue(t, "cannot have column named 'seq'", func() {
		tc.Validate()
	})
}

func TestValidateNonPtr(t *testing.T) {
	db, _ := NewMockProvider().UTInit()
	tc := &CrudBase[*TestCRUDable]{
		DB:          &db.Database,
		NewInstance: func() *TestCRUDable { return &TestCRUDable{} },
		Columns:     []string{"test1"},
		GetFieldPtr: func(inst *TestCRUDable, col string) interface{} {
			return "not a pointer"
		},
	}
	assert.PanicsWithValue(t, "field test1 does not seem to be a pointer type - prevents null-check for PATCH semantics functioning", func() {
		tc.Validate()
	})
}

func TestValidateDupColumn(t *testing.T) {
	db, _ := NewMockProvider().UTInit()
	tc := &CrudBase[*TestCRUDable]{
		DB:          &db.Database,
		NewInstance: func() *TestCRUDable { return &TestCRUDable{} },
		Columns:     []string{"test1", "test1"},
		GetFieldPtr: func(inst *TestCRUDable, col string) interface{} {
			var t *string
			return &t
		},
	}
	assert.PanicsWithValue(t, "test1 is a duplicated column", func() {
		tc.Validate()
	})
}

func TestValidateMissingID(t *testing.T) {
	db, _ := NewMockProvider().UTInit()
	tc := &CrudBase[*TestCRUDable]{
		DB:          &db.Database,
		NewInstance: func() *TestCRUDable { return &TestCRUDable{} },
		Columns:     []string{"test1"},
		GetFieldPtr: func(inst *TestCRUDable, col string) interface{} {
			var t *string
			return &t
		},
	}
	assert.Panics(t, func() {
		tc.Validate()
	})
}

func TestValidateNilValueNonNil(t *testing.T) {
	db, _ := NewMockProvider().UTInit()
	tc := &CrudBase[*TestCRUDable]{
		DB:          &db.Database,
		NewInstance: func() *TestCRUDable { return &TestCRUDable{} },
		Columns:     []string{ColumnID, ColumnCreated, ColumnUpdated},
		GetFieldPtr: func(inst *TestCRUDable, col string) interface{} {
			var t *string
			return &t
		},
		NilValue: func() *TestCRUDable { return &TestCRUDable{} },
	}
	assert.PanicsWithValue(t, "NilValue() value must be nil", func() {
		tc.Validate()
	})
}

func TestValidateScopedFilterNil(t *testing.T) {
	db, _ := NewMockProvider().UTInit()
	tc := &CrudBase[*TestCRUDable]{
		DB:          &db.Database,
		NewInstance: func() *TestCRUDable { return &TestCRUDable{} },
		Columns:     []string{ColumnID, ColumnCreated, ColumnUpdated},
		GetFieldPtr: func(inst *TestCRUDable, col string) interface{} {
			var t *string
			return &t
		},
		NilValue:     func() *TestCRUDable { return nil },
		ScopedFilter: func() sq.Eq { return nil },
	}
	assert.PanicsWithValue(t, "ScopedFilter() value must not be nil", func() {
		tc.Validate()
	})
}

func TestValidateGetFieldPtrNotNilForUnknown(t *testing.T) {
	db, _ := NewMockProvider().UTInit()
	tc := &CrudBase[*TestCRUDable]{
		DB:          &db.Database,
		NewInstance: func() *TestCRUDable { return &TestCRUDable{} },
		Columns:     []string{ColumnID, ColumnCreated, ColumnUpdated},
		GetFieldPtr: func(inst *TestCRUDable, col string) interface{} {
			var t *string
			return &t
		},
		NilValue:     func() *TestCRUDable { return nil },
		ScopedFilter: func() sq.Eq { return sq.Eq{} },
	}
	assert.PanicsWithValue(t, "GetFieldPtr() must return nil for unknown column", func() {
		tc.Validate()
	})
}
