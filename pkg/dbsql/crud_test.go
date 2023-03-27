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
	"encoding/json"
	"fmt"
	"testing"

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
	events []ChangeEventType
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
				return nil
			},
		},
	}
	tc.CrudBase.EventHandler = func(inst *TestCRUDable, eventType ChangeEventType) {
		tc.events = append(tc.events, eventType)
	}
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
	err := tc.Insert(ctx, c1)
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
	err = tc.Upsert(ctx, c1copy, UpsertOptimizationNew)
	assert.NoError(t, err)
	c1copy2, err := tc.GetByID(ctx, c1.ID)
	assert.NoError(t, err)
	checkJSONEq(t, c1copy, c1copy2)

	// Explicitly replace it
	c1copy.Field1 = "hello again - 3"
	err = tc.Replace(ctx, c1copy)
	assert.NoError(t, err)
	c1copy3, err := tc.GetByID(ctx, c1.ID)
	assert.NoError(t, err)
	checkJSONEq(t, c1copy, c1copy3)

	// Explicitly update it
	c1copy.Field1 = "hello again - 4"
	err = tc.Update(ctx, c1copy.ID, CRUDableQueryFactory.NewUpdate(ctx).Set(
		"f1", c1copy.Field1,
	))
	assert.NoError(t, err)
	c1copy4, err := tc.GetByID(ctx, c1.ID)
	assert.NoError(t, err)
	checkJSONEq(t, c1copy, c1copy4)

	// Cannot replace something that doesn't exist
	c2 := *c1
	c2.ID = fftypes.NewUUID()
	c2.Field1 = "bonjour"
	err = tc.Replace(ctx, &c2)
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
	err = tc.InsertBatch(ctx, bunchOfCRUDables, false)
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
	))
	assert.NoError(t, err)
	checkJSONEq(t, bunchOfCRUDables[4], bunch5copy)

	for i := range bunchOfCRUDables {
		ci, err := tc.GetByID(ctx, bunchOfCRUDables[i].ID)
		assert.NoError(t, err)
		assert.Equal(t, int64(929292), ci.Field2.Int64())
	}

	// Delete it
	err = tc.Delete(ctx, bunchOfCRUDables[4].ID)
	assert.NoError(t, err)

	// Check it's gone
	goneOne, err := tc.GetByID(ctx, bunchOfCRUDables[4].ID)
	assert.NoError(t, err)
	assert.Nil(t, goneOne)

}

// func TestUpsertMessageFailBegin(t *testing.T) {
// 	s, mock := newMockProvider().init()
// 	mock.ExpectBegin().WillReturnError(fmt.Errorf("pop"))
// 	err := s.UpsertMessage(context.Background(), &core.Message{}, database.UpsertOptimizationSkip)
// 	assert.Regexp(t, "FF00175", err)
// 	assert.NoError(t, mock.ExpectationsWereMet())
// }

// func TestUpsertMessageFailSelect(t *testing.T) {
// 	s, mock := newMockProvider().init()
// 	mock.ExpectBegin()
// 	mock.ExpectQuery("SELECT .*").WillReturnError(fmt.Errorf("pop"))
// 	mock.ExpectRollback()
// 	msgID := fftypes.NewUUID()
// 	err := s.UpsertMessage(context.Background(), &core.Message{Header: core.MessageHeader{ID: msgID}}, database.UpsertOptimizationSkip)
// 	assert.Regexp(t, "FF00176", err)
// 	assert.NoError(t, mock.ExpectationsWereMet())
// }

// func TestUpsertMessageFailInsert(t *testing.T) {
// 	s, mock := newMockProvider().init()
// 	mock.ExpectBegin()
// 	mock.ExpectQuery("SELECT .*").WillReturnRows(sqlmock.NewRows([]string{}))
// 	mock.ExpectExec("INSERT .*").WillReturnError(fmt.Errorf("pop"))
// 	mock.ExpectRollback()
// 	msgID := fftypes.NewUUID()
// 	err := s.UpsertMessage(context.Background(), &core.Message{Header: core.MessageHeader{ID: msgID}}, database.UpsertOptimizationSkip)
// 	assert.Regexp(t, "FF00177", err)
// 	assert.NoError(t, mock.ExpectationsWereMet())
// }

// func TestUpsertMessageFailUpdate(t *testing.T) {
// 	s, mock := newMockProvider().init()
// 	msgID := fftypes.NewUUID()
// 	mock.ExpectBegin()
// 	mock.ExpectQuery("SELECT .*").WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(msgID.String()))
// 	mock.ExpectExec("UPDATE .*").WillReturnError(fmt.Errorf("pop"))
// 	mock.ExpectRollback()
// 	err := s.UpsertMessage(context.Background(), &core.Message{Header: core.MessageHeader{ID: msgID}}, database.UpsertOptimizationSkip)
// 	assert.Regexp(t, "FF00178", err)
// 	assert.NoError(t, mock.ExpectationsWereMet())
// }

// func TestUpsertMessageFailUpdateRefs(t *testing.T) {
// 	s, mock := newMockProvider().init()
// 	msgID := fftypes.NewUUID()
// 	mock.ExpectBegin()
// 	mock.ExpectQuery("SELECT .*").WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(msgID.String()))
// 	mock.ExpectExec("UPDATE .*").WillReturnResult(driver.ResultNoRows)
// 	mock.ExpectExec("DELETE .*").WillReturnError(fmt.Errorf("pop"))
// 	mock.ExpectRollback()
// 	err := s.UpsertMessage(context.Background(), &core.Message{Header: core.MessageHeader{ID: msgID}}, database.UpsertOptimizationSkip)
// 	assert.Regexp(t, "FF00179", err)
// 	assert.NoError(t, mock.ExpectationsWereMet())
// }

// func TestUpsertMessageFailCommit(t *testing.T) {
// 	s, mock := newMockProvider().init()
// 	msgID := fftypes.NewUUID()
// 	mock.ExpectBegin()
// 	mock.ExpectQuery("SELECT .*").WillReturnRows(sqlmock.NewRows([]string{"id"}))
// 	mock.ExpectExec("INSERT .*").WillReturnResult(sqlmock.NewResult(1, 1))
// 	mock.ExpectCommit().WillReturnError(fmt.Errorf("pop"))
// 	err := s.UpsertMessage(context.Background(), &core.Message{Header: core.MessageHeader{ID: msgID}}, database.UpsertOptimizationSkip)
// 	assert.Regexp(t, "FF00180", err)
// 	assert.NoError(t, mock.ExpectationsWereMet())
// }

// func TestInsertMessagesBeginFail(t *testing.T) {
// 	s, mock := newMockProvider().init()
// 	mock.ExpectBegin().WillReturnError(fmt.Errorf("pop"))
// 	err := s.InsertMessages(context.Background(), []*core.Message{})
// 	assert.Regexp(t, "FF00175", err)
// 	assert.NoError(t, mock.ExpectationsWereMet())
// 	s.callbacks.AssertExpectations(t)
// }

// func TestInsertMessagesMultiRowOK(t *testing.T) {
// 	s := newMockProvider()
// 	s.multiRowInsert = true
// 	s.fakePSQLInsert = true
// 	s, mock := s.init()

// 	msg1 := &core.Message{Header: core.MessageHeader{ID: fftypes.NewUUID(), Namespace: "ns1"}, LocalNamespace: "ns1", Data: core.DataRefs{{ID: fftypes.NewUUID()}}}
// 	msg2 := &core.Message{Header: core.MessageHeader{ID: fftypes.NewUUID(), Namespace: "ns1"}, LocalNamespace: "ns1", Data: core.DataRefs{{ID: fftypes.NewUUID()}}}
// 	s.callbacks.On("OrderedUUIDCollectionNSEvent", database.CollectionMessages, core.ChangeEventTypeCreated, "ns1", msg1.Header.ID, int64(1001))
// 	s.callbacks.On("OrderedUUIDCollectionNSEvent", database.CollectionMessages, core.ChangeEventTypeCreated, "ns1", msg2.Header.ID, int64(1002))

// 	mock.ExpectBegin()
// 	mock.ExpectQuery("INSERT.*messages").WillReturnRows(sqlmock.NewRows([]string{s.SequenceColumn()}).
// 		AddRow(int64(1001)).
// 		AddRow(int64(1002)),
// 	)
// 	mock.ExpectQuery("INSERT.*messages_data").WillReturnRows(sqlmock.NewRows([]string{s.SequenceColumn()}).
// 		AddRow(int64(1003)).
// 		AddRow(int64(1004)),
// 	)
// 	mock.ExpectCommit()
// 	hookCalled := make(chan struct{}, 1)
// 	err := s.InsertMessages(context.Background(), []*core.Message{msg1, msg2}, func() {
// 		close(hookCalled)
// 	})
// 	<-hookCalled
// 	assert.NoError(t, err)
// 	assert.NoError(t, mock.ExpectationsWereMet())
// 	s.callbacks.AssertExpectations(t)
// }

// func TestInsertMessagesMultiRowDataRefsFail(t *testing.T) {
// 	s := newMockProvider()
// 	s.multiRowInsert = true
// 	s.fakePSQLInsert = true
// 	s, mock := s.init()

// 	msg1 := &core.Message{Header: core.MessageHeader{ID: fftypes.NewUUID(), Namespace: "ns1"}, Data: core.DataRefs{{ID: fftypes.NewUUID()}}}

// 	mock.ExpectBegin()
// 	mock.ExpectQuery("INSERT.*messages").WillReturnRows(sqlmock.NewRows([]string{s.SequenceColumn()}).AddRow(int64(1001)))
// 	mock.ExpectQuery("INSERT.*messages_data").WillReturnError(fmt.Errorf("pop"))
// 	err := s.InsertMessages(context.Background(), []*core.Message{msg1})
// 	assert.Regexp(t, "FF00177", err)
// 	assert.NoError(t, mock.ExpectationsWereMet())
// 	s.callbacks.AssertExpectations(t)
// }

// func TestInsertMessagesMultiRowFail(t *testing.T) {
// 	s := newMockProvider()
// 	s.multiRowInsert = true
// 	s.fakePSQLInsert = true
// 	s, mock := s.init()
// 	msg1 := &core.Message{Header: core.MessageHeader{ID: fftypes.NewUUID(), Namespace: "ns1"}}
// 	mock.ExpectBegin()
// 	mock.ExpectQuery("INSERT.*").WillReturnError(fmt.Errorf("pop"))
// 	err := s.InsertMessages(context.Background(), []*core.Message{msg1})
// 	assert.Regexp(t, "FF00177", err)
// 	assert.NoError(t, mock.ExpectationsWereMet())
// 	s.callbacks.AssertExpectations(t)
// }

// func TestInsertMessagesSingleRowFail(t *testing.T) {
// 	s, mock := newMockProvider().init()
// 	msg1 := &core.Message{Header: core.MessageHeader{ID: fftypes.NewUUID(), Namespace: "ns1"}}
// 	mock.ExpectBegin()
// 	mock.ExpectExec("INSERT.*").WillReturnError(fmt.Errorf("pop"))
// 	err := s.InsertMessages(context.Background(), []*core.Message{msg1})
// 	assert.Regexp(t, "FF00177", err)
// 	assert.NoError(t, mock.ExpectationsWereMet())
// 	s.callbacks.AssertExpectations(t)
// }

// func TestInsertMessagesSingleRowFailDataRefs(t *testing.T) {
// 	s, mock := newMockProvider().init()
// 	msg1 := &core.Message{Header: core.MessageHeader{ID: fftypes.NewUUID(), Namespace: "ns1"}, Data: core.DataRefs{{ID: fftypes.NewUUID(), Hash: fftypes.NewRandB32()}}}
// 	mock.ExpectBegin()
// 	mock.ExpectExec("INSERT.*messages").WillReturnResult(sqlmock.NewResult(1, 1))
// 	mock.ExpectExec("INSERT.*messages_data").WillReturnError(fmt.Errorf("pop"))
// 	err := s.InsertMessages(context.Background(), []*core.Message{msg1})
// 	assert.Regexp(t, "FF00177", err)
// 	assert.NoError(t, mock.ExpectationsWereMet())
// 	s.callbacks.AssertExpectations(t)
// }

// func TestReplaceMessageFailBegin(t *testing.T) {
// 	s, mock := newMockProvider().init()
// 	mock.ExpectBegin().WillReturnError(fmt.Errorf("pop"))
// 	msgID := fftypes.NewUUID()
// 	err := s.ReplaceMessage(context.Background(), &core.Message{Header: core.MessageHeader{ID: msgID}})
// 	assert.Regexp(t, "FF00175", err)
// 	assert.NoError(t, mock.ExpectationsWereMet())
// }

// func TestReplaceMessageFailDelete(t *testing.T) {
// 	s, mock := newMockProvider().init()
// 	mock.ExpectBegin()
// 	mock.ExpectExec("DELETE .*").WillReturnError(fmt.Errorf("pop"))
// 	mock.ExpectRollback()
// 	msgID := fftypes.NewUUID()
// 	err := s.ReplaceMessage(context.Background(), &core.Message{Header: core.MessageHeader{ID: msgID}})
// 	assert.Regexp(t, "FF00179", err)
// 	assert.NoError(t, mock.ExpectationsWereMet())
// }

// func TestReplaceMessageFailInsert(t *testing.T) {
// 	s, mock := newMockProvider().init()
// 	mock.ExpectBegin()
// 	mock.ExpectExec("DELETE .*").WillReturnResult(sqlmock.NewResult(1, 1))
// 	mock.ExpectExec("INSERT .*").WillReturnError(fmt.Errorf("pop"))
// 	mock.ExpectRollback()
// 	msgID := fftypes.NewUUID()
// 	err := s.ReplaceMessage(context.Background(), &core.Message{Header: core.MessageHeader{ID: msgID}})
// 	assert.Regexp(t, "FF00177", err)
// 	assert.NoError(t, mock.ExpectationsWereMet())
// }

// func TestUpdateMessageDataRefsNilID(t *testing.T) {
// 	s, mock := newMockProvider().init()
// 	msgID := fftypes.NewUUID()
// 	mock.ExpectBegin()
// 	ctx, tx, _, err := s.BeginOrUseTx(context.Background())
// 	assert.NoError(t, err)
// 	err = s.updateMessageDataRefs(ctx, tx, &core.Message{
// 		Header: core.MessageHeader{ID: msgID},
// 		Data:   []*core.DataRef{{ID: nil}},
// 	}, false)
// 	assert.Regexp(t, "FF10123", err)
// 	assert.NoError(t, mock.ExpectationsWereMet())
// }

// func TestUpdateMessageDataRefsNilHash(t *testing.T) {
// 	s, mock := newMockProvider().init()
// 	msgID := fftypes.NewUUID()
// 	mock.ExpectBegin()
// 	ctx, tx, _, err := s.BeginOrUseTx(context.Background())
// 	assert.NoError(t, err)
// 	err = s.updateMessageDataRefs(ctx, tx, &core.Message{
// 		Header: core.MessageHeader{ID: msgID},
// 		Data:   []*core.DataRef{{ID: fftypes.NewUUID()}},
// 	}, false)
// 	assert.Regexp(t, "FF10139", err)
// 	assert.NoError(t, mock.ExpectationsWereMet())
// }

// func TestUpdateMessageDataDeleteFail(t *testing.T) {
// 	s, mock := newMockProvider().init()
// 	msgID := fftypes.NewUUID()
// 	mock.ExpectBegin()
// 	ctx, tx, _, err := s.BeginOrUseTx(context.Background())
// 	assert.NoError(t, err)
// 	mock.ExpectExec("DELETE .*").WillReturnError(fmt.Errorf("pop"))
// 	err = s.updateMessageDataRefs(ctx, tx, &core.Message{
// 		Header: core.MessageHeader{ID: msgID},
// 	}, true)
// 	assert.Regexp(t, "FF00179", err)
// 	assert.NoError(t, mock.ExpectationsWereMet())
// }

// func TestUpdateMessageDataAddFail(t *testing.T) {
// 	s, mock := newMockProvider().init()
// 	msgID := fftypes.NewUUID()
// 	dataID := fftypes.NewUUID()
// 	dataHash := fftypes.NewRandB32()
// 	mock.ExpectBegin()
// 	ctx, tx, _, err := s.BeginOrUseTx(context.Background())
// 	assert.NoError(t, err)
// 	mock.ExpectExec("INSERT .*").WillReturnError(fmt.Errorf("pop"))
// 	err = s.updateMessageDataRefs(ctx, tx, &core.Message{
// 		Header: core.MessageHeader{ID: msgID},
// 		Data:   []*core.DataRef{{ID: dataID, Hash: dataHash}},
// 	}, false)
// 	assert.Regexp(t, "FF00177", err)
// 	assert.NoError(t, mock.ExpectationsWereMet())
// }

// func TestLoadMessageDataRefsQueryFail(t *testing.T) {
// 	s, mock := newMockProvider().init()
// 	msgID := fftypes.NewUUID()
// 	mock.ExpectQuery("SELECT .*").WillReturnError(fmt.Errorf("pop"))
// 	err := s.loadDataRefs(context.Background(), "ns1", []*core.Message{
// 		{
// 			Header: core.MessageHeader{ID: msgID},
// 		},
// 	})
// 	assert.Regexp(t, "FF00176", err)
// 	assert.NoError(t, mock.ExpectationsWereMet())
// }

// func TestLoadMessageDataRefsScanFail(t *testing.T) {
// 	s, mock := newMockProvider().init()
// 	msgID := fftypes.NewUUID()
// 	mock.ExpectQuery("SELECT .*").WillReturnRows(sqlmock.NewRows([]string{"data_id"}).AddRow("only one"))
// 	err := s.loadDataRefs(context.Background(), "ns1", []*core.Message{
// 		{
// 			Header: core.MessageHeader{ID: msgID},
// 		},
// 	})
// 	assert.Regexp(t, "FF10121", err)
// 	assert.NoError(t, mock.ExpectationsWereMet())
// }

// func TestLoadMessageDataRefsEmpty(t *testing.T) {
// 	s, mock := newMockProvider().init()
// 	msgID := fftypes.NewUUID()
// 	msg := &core.Message{Header: core.MessageHeader{ID: msgID}}
// 	mock.ExpectQuery("SELECT .*").WillReturnRows(sqlmock.NewRows([]string{"data_id", "data_hash"}))
// 	err := s.loadDataRefs(context.Background(), "ns1", []*core.Message{msg})
// 	assert.NoError(t, err)
// 	assert.Equal(t, core.DataRefs{}, msg.Data)
// 	assert.NoError(t, mock.ExpectationsWereMet())
// }

// func TestGetMessageByIDSelectFail(t *testing.T) {
// 	s, mock := newMockProvider().init()
// 	msgID := fftypes.NewUUID()
// 	mock.ExpectQuery("SELECT .*").WillReturnError(fmt.Errorf("pop"))
// 	_, err := s.GetMessageByID(context.Background(), "ns1", msgID)
// 	assert.Regexp(t, "FF00176", err)
// 	assert.NoError(t, mock.ExpectationsWereMet())
// }

// func TestGetMessageByIDNotFound(t *testing.T) {
// 	s, mock := newMockProvider().init()
// 	msgID := fftypes.NewUUID()
// 	mock.ExpectQuery("SELECT .*").WillReturnRows(sqlmock.NewRows([]string{"id"}))
// 	msg, err := s.GetMessageByID(context.Background(), "ns1", msgID)
// 	assert.NoError(t, err)
// 	assert.Nil(t, msg)
// 	assert.NoError(t, mock.ExpectationsWereMet())
// }

// func TestGetMessageByIDScanFail(t *testing.T) {
// 	s, mock := newMockProvider().init()
// 	msgID := fftypes.NewUUID()
// 	mock.ExpectQuery("SELECT .*").WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow("only one"))
// 	_, err := s.GetMessageByID(context.Background(), "ns1", msgID)
// 	assert.Regexp(t, "FF10121", err)
// 	assert.NoError(t, mock.ExpectationsWereMet())
// }

// func TestGetMessageByIDLoadRefsFail(t *testing.T) {
// 	s, mock := newMockProvider().init()
// 	msgID := fftypes.NewUUID()
// 	b32 := fftypes.NewRandB32()
// 	cols := append([]string{}, msgColumns...)
// 	cols = append(cols, "id()")
// 	mock.ExpectQuery("SELECT .*").WillReturnRows(sqlmock.NewRows(cols).
// 		AddRow(msgID.String(), nil, core.MessageTypeBroadcast, "author1", "0x12345", 0, "ns1", "ns1", "t1", "c1", nil, b32.String(), b32.String(), b32.String(), "confirmed", 0, "pin", nil, "", nil, nil, "bob", 0))
// 	mock.ExpectQuery("SELECT .*").WillReturnError(fmt.Errorf("pop"))
// 	_, err := s.GetMessageByID(context.Background(), "ns1", msgID)
// 	assert.Regexp(t, "FF00176", err)
// 	assert.NoError(t, mock.ExpectationsWereMet())
// }

// func TestGetMessagesBuildQueryFail(t *testing.T) {
// 	s, _ := newMockProvider().init()
// 	f := database.MessageQueryFactory.NewFilter(context.Background()).Eq("id", map[bool]bool{true: false})
// 	_, _, err := s.GetMessages(context.Background(), "ns1", f)
// 	assert.Regexp(t, "FF00143.*id", err)
// }

// func TestGetMessagesQueryFail(t *testing.T) {
// 	s, mock := newMockProvider().init()
// 	mock.ExpectQuery("SELECT .*").WillReturnError(fmt.Errorf("pop"))
// 	f := database.MessageQueryFactory.NewFilter(context.Background()).Eq("id", "")
// 	_, _, err := s.GetMessages(context.Background(), "ns1", f)
// 	assert.Regexp(t, "FF00176", err)
// 	assert.NoError(t, mock.ExpectationsWereMet())
// }

// func TestGetMessagesForDataBadQuery(t *testing.T) {
// 	s, mock := newMockProvider().init()
// 	f := database.MessageQueryFactory.NewFilter(context.Background()).Eq("!wrong", "")
// 	_, _, err := s.GetMessagesForData(context.Background(), "ns1", fftypes.NewUUID(), f)
// 	assert.Regexp(t, "FF00142", err)
// 	assert.NoError(t, mock.ExpectationsWereMet())
// }

// func TestGetMessagesReadMessageFail(t *testing.T) {
// 	s, mock := newMockProvider().init()
// 	mock.ExpectQuery("SELECT .*").WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow("only one"))
// 	f := database.MessageQueryFactory.NewFilter(context.Background()).Eq("id", "")
// 	_, _, err := s.GetMessages(context.Background(), "ns1", f)
// 	assert.Regexp(t, "FF10121", err)
// 	assert.NoError(t, mock.ExpectationsWereMet())
// }

// func TestGetMessagesLoadRefsFail(t *testing.T) {
// 	s, mock := newMockProvider().init()
// 	msgID := fftypes.NewUUID()
// 	b32 := fftypes.NewRandB32()
// 	cols := append([]string{}, msgColumns...)
// 	cols = append(cols, "id()")
// 	mock.ExpectQuery("SELECT .*").WillReturnRows(sqlmock.NewRows(cols).
// 		AddRow(msgID.String(), nil, core.MessageTypeBroadcast, "author1", "0x12345", 0, "ns1", "ns1", "t1", "c1", nil, b32.String(), b32.String(), b32.String(), "confirmed", 0, "pin", nil, "", nil, nil, "bob", 0))
// 	mock.ExpectQuery("SELECT .*").WillReturnError(fmt.Errorf("pop"))
// 	f := database.MessageQueryFactory.NewFilter(context.Background()).Gt("confirmed", "0")
// 	_, _, err := s.GetMessages(context.Background(), "ns1", f)
// 	assert.Regexp(t, "FF00176", err)
// 	assert.NoError(t, mock.ExpectationsWereMet())
// }

// func TestMessageUpdateBeginFail(t *testing.T) {
// 	s, mock := newMockProvider().init()
// 	mock.ExpectBegin().WillReturnError(fmt.Errorf("pop"))
// 	u := database.MessageQueryFactory.NewUpdate(context.Background()).Set("id", "anything")
// 	err := s.UpdateMessage(context.Background(), "ns1", fftypes.NewUUID(), u)
// 	assert.Regexp(t, "FF00175", err)
// }

// func TestGetMessageIDsQueryFail(t *testing.T) {
// 	s, mock := newMockProvider().init()
// 	mock.ExpectQuery("SELECT .*").WillReturnError(fmt.Errorf("pop"))
// 	f := database.MessageQueryFactory.NewFilter(context.Background()).Eq("id", "")
// 	_, err := s.GetMessageIDs(context.Background(), "ns1", f)
// 	assert.Regexp(t, "FF00176", err)
// 	assert.NoError(t, mock.ExpectationsWereMet())
// }

// func TestGetMessageIDsReadMessageFail(t *testing.T) {
// 	s, mock := newMockProvider().init()
// 	mock.ExpectQuery("SELECT .*").WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow("bad id"))
// 	f := database.MessageQueryFactory.NewFilter(context.Background()).Eq("id", "")
// 	_, err := s.GetMessageIDs(context.Background(), "ns1", f)
// 	assert.Regexp(t, "FF10121", err)
// 	assert.NoError(t, mock.ExpectationsWereMet())
// }

// func TestGetMessageIDsBadQuery(t *testing.T) {
// 	s, mock := newMockProvider().init()
// 	f := database.MessageQueryFactory.NewFilter(context.Background()).Eq("!wrong", "")
// 	_, err := s.GetMessageIDs(context.Background(), "ns1", f)
// 	assert.Regexp(t, "FF00142", err)
// 	assert.NoError(t, mock.ExpectationsWereMet())
// }

// func TestMessageUpdateBuildQueryFail(t *testing.T) {
// 	s, mock := newMockProvider().init()
// 	mock.ExpectBegin()
// 	u := database.MessageQueryFactory.NewUpdate(context.Background()).Set("id", map[bool]bool{true: false})
// 	err := s.UpdateMessage(context.Background(), "ns1", fftypes.NewUUID(), u)
// 	assert.Regexp(t, "FF00143.*id", err)
// }

// func TestMessagesUpdateBuildFilterFail(t *testing.T) {
// 	s, mock := newMockProvider().init()
// 	mock.ExpectBegin()
// 	f := database.MessageQueryFactory.NewFilter(context.Background()).Eq("id", map[bool]bool{true: false})
// 	u := database.MessageQueryFactory.NewUpdate(context.Background()).Set("type", core.MessageTypeBroadcast)
// 	err := s.UpdateMessages(context.Background(), "ns1", f, u)
// 	assert.Regexp(t, "FF00143.*id", err)
// }

// func TestMessageUpdateFail(t *testing.T) {
// 	s, mock := newMockProvider().init()
// 	mock.ExpectBegin()
// 	mock.ExpectExec("UPDATE .*").WillReturnError(fmt.Errorf("pop"))
// 	mock.ExpectRollback()
// 	u := database.MessageQueryFactory.NewUpdate(context.Background()).Set("group", fftypes.NewRandB32())
// 	err := s.UpdateMessage(context.Background(), "ns1", fftypes.NewUUID(), u)
// 	assert.Regexp(t, "FF00178", err)
// }

// func TestGetBatchIDsForMessagesSelectFail(t *testing.T) {
// 	s, mock := newMockProvider().init()
// 	msgID := fftypes.NewUUID()
// 	mock.ExpectQuery("SELECT .*").WillReturnError(fmt.Errorf("pop"))
// 	_, err := s.GetBatchIDsForMessages(context.Background(), "ns1", []*fftypes.UUID{msgID})
// 	assert.Regexp(t, "FF00176", err)
// 	assert.NoError(t, mock.ExpectationsWereMet())
// }

// func TestGetBatchIDsForMessagesScanFail(t *testing.T) {
// 	s, mock := newMockProvider().init()
// 	msgID := fftypes.NewUUID()
// 	mock.ExpectQuery("SELECT .*").WillReturnRows(sqlmock.NewRows([]string{"batch_id"}).AddRow("not a UUID"))
// 	_, err := s.GetBatchIDsForMessages(context.Background(), "ns1", []*fftypes.UUID{msgID})
// 	assert.Regexp(t, "FF10121", err)
// 	assert.NoError(t, mock.ExpectationsWereMet())
// }
