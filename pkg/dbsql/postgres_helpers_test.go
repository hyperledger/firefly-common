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

package dbsql

import (
	"context"
	"database/sql/driver"
	"testing"

	"github.com/hyperledger/firefly-common/pkg/fftypes"
	"github.com/stretchr/testify/assert"
)

func TestBuildPostgreSQLOptimizedUpsert(t *testing.T) {

	now := fftypes.Now()
	q, err := BuildPostgreSQLOptimizedUpsert(context.Background(), "table1", "id", []string{
		"created",
		"updated",
		"mutable_col",
		"immutable_col",
	}, []string{
		"updated",
		"mutable_col",
	}, "created", map[string]driver.Value{
		"created":       now,
		"updated":       now,
		"mutable_col":   "value1",
		"immutable_col": "value2",
	})
	assert.NoError(t, err)

	queryStr, values, err := q.ToSql()
	assert.NoError(t, err)
	assert.Equal(t, "INSERT INTO table1 (created,updated,mutable_col,immutable_col) VALUES (?,?,?,?) ON CONFLICT (id) DO UPDATE SET updated = ?, mutable_col = ? RETURNING created", queryStr)
	assert.Equal(t, []interface{}{
		now, now, "value1", "value2",
		now, "value1",
	}, values)

}

func TestBuildPostgreSQLOptimizedUpsertFail(t *testing.T) {

	_, err := BuildPostgreSQLOptimizedUpsert(context.Background(), "", "", []string{}, []string{}, "", map[string]driver.Value{})
	assert.Regexp(t, "FF00247", err)

}

func TestBuildPostgreSQLArrayInsert(t *testing.T) {
	sqlStr, args, err := BuildPostgreSQLArrayInsert(
		context.Background(),
		"transfers",
		[]string{"id", "sender", "amount"},
		[][]interface{}{
			{"id1", "id2"},
			{"0xaaa", "0xbbb"},
			{100, 200},
		},
		"seq",
	)
	assert.NoError(t, err)
	assert.Equal(t, "INSERT INTO transfers (id, sender, amount) SELECT UNNEST($1), UNNEST($2), UNNEST($3) RETURNING seq", sqlStr)
	assert.Len(t, args, 3)
	// Args are mockArrayValue wrappers for database/sql compatibility
	for _, arg := range args {
		_, ok := arg.(mockArrayValue)
		assert.True(t, ok, "expected mockArrayValue, got %T", arg)
	}
}

func TestPostgreSQLArrayInsertBuilderWithCustomWrapper(t *testing.T) {
	wrapCalled := 0
	builder := PostgreSQLArrayInsertBuilder(func(vals []interface{}) interface{} {
		wrapCalled++
		return vals
	})
	sql, args, err := builder(
		context.Background(),
		"things",
		[]string{"a", "b"},
		[][]interface{}{
			{1, 2, 3},
			{"x", "y", "z"},
		},
		"seq",
	)
	assert.NoError(t, err)
	assert.Equal(t, "INSERT INTO things (a, b) SELECT UNNEST($1), UNNEST($2) RETURNING seq", sql)
	assert.Len(t, args, 2)
	assert.Equal(t, 2, wrapCalled)
}
