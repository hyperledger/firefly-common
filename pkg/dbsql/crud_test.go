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
	ID     *fftypes.UUID    `json:"id"`
	NS     string           `json:"namespace"`
	Field1 string           `json:"f1"`
	Field2 fftypes.FFBigInt `json:"f2"`
	Field3 *fftypes.JSONAny `json:"f3"`
}

var CRUDableQueryFactory = &ffapi.QueryFields{
	"id": &ffapi.UUIDField{},
	"f1": &ffapi.StringField{},
	"f2": &ffapi.BigIntField{},
	"f3": &ffapi.JSONField{},
}

func (tc *TestCRUDable) GetID() *fftypes.UUID {
	return tc.ID
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
				"id",
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
				case "id":
					return &inst.ID
				case "ns":
					return &inst.NS
				case "field1":
					return &inst.Field1
				case "field2":
					return &inst.Field2
				case "field3":
					return &inst.Field3
				}
				panic(fmt.Sprintf("unknown column: '%s'", col))
			},
		},
	}
	tc.CrudBase.EventHandler = func(inst *TestCRUDable, eventType ChangeEventType) {
		tc.events = append(tc.events, eventType)
	}
	tc.postCommit = func() { tc.postCommits++ }
	return tc
}

func checkJSONEq(t *testing.T, o1, o2 interface{}) {
	j1, err := json.Marshal(o1)
	assert.NoError(t, err)
	j2, err := json.Marshal(o1)
	assert.NoError(t, err)
	assert.JSONEq(t, string(j1), string(j2))
}

func TestCRUDWithDBEnd2End(t *testing.T) {
	log.SetLevel("trace")

	db, done := newSQLiteTestProvider(t)
	defer done()
	ctx := context.Background()

	c1 := &TestCRUDable{
		ID:     fftypes.NewUUID(),
		NS:     "ns1",
		Field1: "hello1",
		Field2: *fftypes.NewFFBigInt(12345),
		Field3: fftypes.JSONAnyPtr(`{"some":"stuff"}`),
	}

	tc := newCRUDCollection(&db.Database, "ns1")

	// Add a row
	err := tc.Insert(ctx, c1, tc.postCommit)
	assert.NoError(t, err)
	assert.Len(t, tc.events, 1)
	assert.Equal(t, Created, tc.events[0])
	tc.events = nil

	// Check we get it back
	c1copy, err := tc.GetByID(ctx, c1.ID)
	assert.NoError(t, err)
	checkJSONEq(t, c1, c1copy)

	// Upsert the existing row optimized
	c1copy.Field1 = "hello again - 1"
	err = tc.Upsert(ctx, c1copy, UpsertOptimizationExisting)
	assert.NoError(t, err)
	c1copy1, err := tc.GetByID(ctx, c1.ID)
	assert.NoError(t, err)
	checkJSONEq(t, c1copy, c1copy1)
	assert.Len(t, tc.events, 1)
	assert.Equal(t, Updated, tc.events[0])
	tc.events = nil

	// Upsert the existing row un-optimized
	c1copy.Field1 = "hello again - 2"
	err = tc.Upsert(ctx, c1copy, UpsertOptimizationNew, tc.postCommit)
	assert.NoError(t, err)
	c1copy2, err := tc.GetByID(ctx, c1.ID)
	assert.NoError(t, err)
	checkJSONEq(t, c1copy, c1copy2)

	// Explicitly replace it
	c1copy.Field1 = "hello again - 3"
	err = tc.Replace(ctx, c1copy, tc.postCommit)
	assert.NoError(t, err)
	c1copy3, err := tc.GetByID(ctx, c1.ID)
	assert.NoError(t, err)
	checkJSONEq(t, c1copy, c1copy3)

	// Explicitly update it
	c1copy.Field1 = "hello again - 4"
	err = tc.Update(ctx, c1copy.ID, CRUDableQueryFactory.NewUpdate(ctx).Set(
		"f1", c1copy.Field1,
	), tc.postCommit)
	assert.NoError(t, err)
	c1copy4, err := tc.GetByID(ctx, c1.ID)
	assert.NoError(t, err)
	checkJSONEq(t, c1copy, c1copy4)

	// Cannot replace something that doesn't exist
	c2 := *c1
	c2.ID = fftypes.NewUUID()
	c2.Field1 = "bonjour"
	err = tc.Replace(ctx, &c2, tc.postCommit)
	assert.Regexp(t, "FF00205", err)

	// Optimized insert of another
	err = tc.Upsert(ctx, &c2, UpsertOptimizationNew)
	assert.NoError(t, err)
	c2copy1, err := tc.GetByID(ctx, c1.ID)
	assert.NoError(t, err)
	checkJSONEq(t, c2, c2copy1)

	// Check we can filter it with the new value
	cs, _, err := tc.GetMany(ctx, CRUDableQueryFactory.NewFilter(ctx).Eq(
		"f1", "bonjour",
	))
	assert.NoError(t, err)
	assert.Len(t, cs, 1)
	checkJSONEq(t, c2, cs[0])

	// Insert a bunch in a batch
	bunchOfCRUDables := make([]*TestCRUDable, 10)
	for i := range bunchOfCRUDables {
		bunchOfCRUDables[i] = &TestCRUDable{
			ID:     fftypes.NewUUID(),
			NS:     "ns1",
			Field1: fmt.Sprintf("crudable[%.5d]", i),
			Field2: *fftypes.NewFFBigInt(919191),
		}
	}
	err = tc.InsertMany(ctx, bunchOfCRUDables, false, tc.postCommit)
	assert.NoError(t, err)

	// Grab one
	bunch5copy, err := tc.GetByID(ctx, bunchOfCRUDables[4].ID)
	assert.NoError(t, err)
	checkJSONEq(t, bunchOfCRUDables[4], bunch5copy)

	// Update them all
	err = tc.UpdateMany(ctx, CRUDableQueryFactory.NewFilter(ctx).Eq(
		"f2", "919191",
	), CRUDableQueryFactory.NewUpdate(ctx).Set(
		"f2", "929292",
	), tc.postCommit)
	assert.NoError(t, err)
	checkJSONEq(t, bunchOfCRUDables[4], bunch5copy)

	for i := range bunchOfCRUDables {
		ci, err := tc.GetByID(ctx, bunchOfCRUDables[i].ID)
		assert.NoError(t, err)
		assert.Equal(t, int64(929292), ci.Field2.Int64())
	}

	// Delete it
	err = tc.Delete(ctx, bunchOfCRUDables[4].ID, tc.postCommit)
	assert.NoError(t, err)

	// Check it's gone
	goneOne, err := tc.GetByID(ctx, bunchOfCRUDables[4].ID)
	assert.NoError(t, err)
	assert.Nil(t, goneOne)

	// Check all the post commits above fired
	assert.Equal(t, 7, tc.postCommits)

}

func TestUpsertFailBegin(t *testing.T) {
	db, mock := newMockProvider().init()
	tc := newCRUDCollection(&db.Database, "ns1")
	mock.ExpectBegin().WillReturnError(fmt.Errorf("pop"))
	err := tc.Upsert(context.Background(), &TestCRUDable{}, UpsertOptimizationSkip)
	assert.Regexp(t, "FF00175", err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestUpsertFailQuery(t *testing.T) {
	db, mock := newMockProvider().init()
	tc := newCRUDCollection(&db.Database, "ns1")
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT.*").WillReturnError(fmt.Errorf("pop"))
	err := tc.Upsert(context.Background(), &TestCRUDable{}, UpsertOptimizationSkip)
	assert.Regexp(t, "FF00176", err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestUpsertFailInsert(t *testing.T) {
	db, mock := newMockProvider().init()
	tc := newCRUDCollection(&db.Database, "ns1")
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT.*").WillReturnRows(mock.NewRows([]string{db.sequenceColumn}))
	mock.ExpectExec("INSERT.*").WillReturnError(fmt.Errorf("pop"))
	err := tc.Upsert(context.Background(), &TestCRUDable{}, UpsertOptimizationSkip)
	assert.Regexp(t, "FF00177", err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestUpsertFailUpdate(t *testing.T) {
	db, mock := newMockProvider().init()
	tc := newCRUDCollection(&db.Database, "ns1")
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT.*").WillReturnRows(mock.NewRows([]string{db.sequenceColumn}).AddRow(12345))
	mock.ExpectExec("UPDATE.*").WillReturnError(fmt.Errorf("pop"))
	err := tc.Upsert(context.Background(), &TestCRUDable{}, UpsertOptimizationSkip)
	assert.Regexp(t, "FF00178", err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestInsertManyBeginFail(t *testing.T) {
	db, mock := newMockProvider().init()
	tc := newCRUDCollection(&db.Database, "ns1")
	mock.ExpectBegin().WillReturnError(fmt.Errorf("pop"))
	err := tc.InsertMany(context.Background(), []*TestCRUDable{{}}, false)
	assert.Regexp(t, "FF00175", err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestInsertManyMultiRowOK(t *testing.T) {
	db, mock := newMockProvider().init()
	db.fakePSQLInsert = true
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
		{ID: fftypes.NewUUID()},
		{ID: fftypes.NewUUID()},
	}, false)
	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestInsertManyMultiRowFail(t *testing.T) {
	db, mock := newMockProvider().init()
	db.fakePSQLInsert = true
	db.features.MultiRowInsert = true
	tc := newCRUDCollection(&db.Database, "ns1")
	mock.ExpectBegin()
	mock.ExpectQuery(`INSERT INTO crudables.*VALUES \(.*\),\(.*\)`).WillReturnError(fmt.Errorf("pop"))
	err := tc.InsertMany(context.Background(), []*TestCRUDable{
		{ID: fftypes.NewUUID()},
		{ID: fftypes.NewUUID()},
	}, false)
	assert.Regexp(t, "FF00177", err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestInsertManyFallbackSingleRowFail(t *testing.T) {
	db, mock := newMockProvider().init()
	tc := newCRUDCollection(&db.Database, "ns1")
	mock.ExpectBegin()
	mock.ExpectExec(`INSERT INTO crudables.*`).WillReturnError(fmt.Errorf("pop"))
	err := tc.InsertMany(context.Background(), []*TestCRUDable{
		{ID: fftypes.NewUUID()},
		{ID: fftypes.NewUUID()},
	}, false)
	assert.Regexp(t, "FF00177", err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestInsertBeginFail(t *testing.T) {
	db, mock := newMockProvider().init()
	tc := newCRUDCollection(&db.Database, "ns1")
	mock.ExpectBegin().WillReturnError(fmt.Errorf("pop"))
	err := tc.Insert(context.Background(), &TestCRUDable{})
	assert.Regexp(t, "FF00175", err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestInsertInsertFail(t *testing.T) {
	db, mock := newMockProvider().init()
	tc := newCRUDCollection(&db.Database, "ns1")
	mock.ExpectBegin()
	mock.ExpectExec("INSERT.*").WillReturnError(fmt.Errorf("pop"))
	err := tc.Insert(context.Background(), &TestCRUDable{})
	assert.Regexp(t, "FF00177", err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestReplaceBeginFail(t *testing.T) {
	db, mock := newMockProvider().init()
	tc := newCRUDCollection(&db.Database, "ns1")
	mock.ExpectBegin().WillReturnError(fmt.Errorf("pop"))
	err := tc.Replace(context.Background(), &TestCRUDable{})
	assert.Regexp(t, "FF00175", err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestReplaceUpdateFail(t *testing.T) {
	db, mock := newMockProvider().init()
	tc := newCRUDCollection(&db.Database, "ns1")
	mock.ExpectBegin()
	mock.ExpectExec("UPDATE.*").WillReturnError(fmt.Errorf("pop"))
	err := tc.Replace(context.Background(), &TestCRUDable{})
	assert.Regexp(t, "FF00178", err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestGetByIDSelectFail(t *testing.T) {
	db, mock := newMockProvider().init()
	tc := newCRUDCollection(&db.Database, "ns1")
	mock.ExpectQuery("SELECT.*").WillReturnError(fmt.Errorf("pop"))
	_, err := tc.GetByID(context.Background(), fftypes.NewUUID())
	assert.Regexp(t, "FF00176", err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestGetByIDScanFail(t *testing.T) {
	db, mock := newMockProvider().init()
	tc := newCRUDCollection(&db.Database, "ns1")
	mock.ExpectQuery("SELECT.*").WillReturnRows(sqlmock.NewRows([]string{}).AddRow())
	_, err := tc.GetByID(context.Background(), fftypes.NewUUID())
	assert.Regexp(t, "FF00182", err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestGetByManySelectFail(t *testing.T) {
	db, mock := newMockProvider().init()
	tc := newCRUDCollection(&db.Database, "ns1")
	mock.ExpectQuery("SELECT.*").WillReturnError(fmt.Errorf("pop"))
	_, _, err := tc.GetMany(context.Background(), CRUDableQueryFactory.NewFilter(context.Background()).And())
	assert.Regexp(t, "FF00176", err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestGetByManyScanFail(t *testing.T) {
	db, mock := newMockProvider().init()
	tc := newCRUDCollection(&db.Database, "ns1")
	mock.ExpectQuery("SELECT.*").WillReturnRows(sqlmock.NewRows([]string{}).AddRow())
	_, _, err := tc.GetMany(context.Background(), CRUDableQueryFactory.NewFilter(context.Background()).And())
	assert.Regexp(t, "FF00182", err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestGetByManyFilterFail(t *testing.T) {
	db, mock := newMockProvider().init()
	tc := newCRUDCollection(&db.Database, "ns1")
	_, _, err := tc.GetMany(context.Background(), CRUDableQueryFactory.NewFilter(context.Background()).Eq("wrong", 123))
	assert.Regexp(t, "FF00142", err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestUpdateBeginFail(t *testing.T) {
	db, mock := newMockProvider().init()
	tc := newCRUDCollection(&db.Database, "ns1")
	mock.ExpectBegin().WillReturnError(fmt.Errorf("pop"))
	mock.ExpectExec("UPDATE.*").WillReturnResult(driver.ResultNoRows)
	err := tc.Update(context.Background(), fftypes.NewUUID(), CRUDableQueryFactory.NewUpdate(context.Background()).Set("f1", "12345"))
	assert.Regexp(t, "FF00175", err)
}

func TestUpdateBuildUpdateFail(t *testing.T) {
	db, mock := newMockProvider().init()
	tc := newCRUDCollection(&db.Database, "ns1")
	mock.ExpectBegin()
	err := tc.UpdateMany(context.Background(),
		CRUDableQueryFactory.NewFilter(context.Background()).Eq("wrong", 123),
		CRUDableQueryFactory.NewUpdate(context.Background()).Set("f1", "12345"))
	assert.Regexp(t, "FF00142", err)
}

func TestUpdateUpdateFail(t *testing.T) {
	db, mock := newMockProvider().init()
	tc := newCRUDCollection(&db.Database, "ns1")
	mock.ExpectBegin()
	mock.ExpectExec("UPDATE.*").WillReturnError(fmt.Errorf("pop"))
	err := tc.Update(context.Background(), fftypes.NewUUID(), CRUDableQueryFactory.NewUpdate(context.Background()).Set("f1", "12345"))
	assert.Regexp(t, "FF00178", err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestUpdateNowRows(t *testing.T) {
	db, mock := newMockProvider().init()
	tc := newCRUDCollection(&db.Database, "ns1")
	mock.ExpectBegin()
	mock.ExpectExec("UPDATE.*").WillReturnResult(driver.ResultNoRows)
	err := tc.Update(context.Background(), fftypes.NewUUID(), CRUDableQueryFactory.NewUpdate(context.Background()).Set("f1", "12345"))
	assert.Regexp(t, "FF00205", err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestDeleteBeginFail(t *testing.T) {
	db, mock := newMockProvider().init()
	tc := newCRUDCollection(&db.Database, "ns1")
	mock.ExpectBegin().WillReturnError(fmt.Errorf("pop"))
	mock.ExpectExec("UPDATE.*").WillReturnResult(driver.ResultNoRows)
	err := tc.Delete(context.Background(), fftypes.NewUUID())
	assert.Regexp(t, "FF00175", err)
}

func TestDeleteDeleteFail(t *testing.T) {
	db, mock := newMockProvider().init()
	tc := newCRUDCollection(&db.Database, "ns1")
	mock.ExpectBegin()
	mock.ExpectExec("DELETE.*").WillReturnError(fmt.Errorf("pop"))
	err := tc.Delete(context.Background(), fftypes.NewUUID())
	assert.Regexp(t, "FF00179", err)
}
