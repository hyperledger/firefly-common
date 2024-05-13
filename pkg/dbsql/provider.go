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
	"database/sql"
	"database/sql/driver"

	sq "github.com/Masterminds/squirrel"
	migratedb "github.com/golang-migrate/migrate/v4/database"
)

type SQLFeatures struct {
	UseILIKE          bool
	MultiRowInsert    bool
	PlaceholderFormat sq.PlaceholderFormat
	AcquireLock       func(lockName string) string
	// DB specific query builder for RDBMS-side optimized upsert, returning the requested column from the query
	// (the CRUD layer will request the create time column to detect if the record was new or not)
	DBOptimizedUpsertBuilder func(ctx context.Context, table string, insertCols, updateCols []string, returnCol string, values map[string]driver.Value) (sq.InsertBuilder, error)
}

func DefaultSQLProviderFeatures() SQLFeatures {
	return SQLFeatures{
		UseILIKE:          false,
		MultiRowInsert:    false,
		PlaceholderFormat: sq.Dollar,
	}
}

// Provider defines the interface an individual provider muse implement to customize the Database implementation
type Provider interface {

	// Name is the name of the database driver
	Name() string

	// MigrationDir is the subdirectory for migrations
	MigrationsDir() string

	// SequenceColumn is the name of the sequence column to use
	SequenceColumn() string

	// Open creates the DB instances
	Open(url string) (*sql.DB, error)

	// GetDriver returns the driver implementation
	GetMigrationDriver(*sql.DB) (migratedb.Driver, error)

	// Features returns database specific configuration switches
	Features() SQLFeatures

	// ApplyInsertQueryCustomizations updates the INSERT query for returning the Sequence, and returns whether it needs to be run as a query to return the Sequence field
	ApplyInsertQueryCustomizations(insert sq.InsertBuilder, requestConflictEmptyResult bool) (updatedInsert sq.InsertBuilder, runAsQuery bool)
}
