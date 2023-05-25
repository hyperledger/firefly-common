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
	"testing"

	"github.com/Masterminds/squirrel"
	"github.com/stretchr/testify/assert"
)

func TestMockProvider(t *testing.T) {
	mp, _ := NewMockProvider().UTInit()

	assert.Equal(t, "mockdb", mp.Name())
	assert.Equal(t, "seq", mp.SequenceColumn())
	assert.Equal(t, "mockdb", mp.MigrationsDir())
	assert.NotNil(t, mp.Features())
	assert.NotEmpty(t, mp.Features().AcquireLock("test1"))

	_, applied := mp.ApplyInsertQueryCustomizations(squirrel.Insert("test"), true)
	assert.False(t, applied)
	mp.FakePSQLInsert = true
	_, applied = mp.ApplyInsertQueryCustomizations(squirrel.Insert("test"), true)
	assert.True(t, applied)

	db, err := mp.Open("test")
	assert.NoError(t, err)
	assert.NotNil(t, db)

	driver, err := mp.GetMigrationDriver(db)
	assert.NoError(t, err)
	assert.NotNil(t, driver)
}
