/*******************************************************************************
 * Copyright (c) 2024 Genome Research Ltd.
 *
 * Author: Sendu Bala <sb10@sanger.ac.uk>
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be included
 * in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
 * CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
 * TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 ******************************************************************************/

package db

import (
	"os"
	"path/filepath"
	"strings"

	es "github.com/wtsi-hgi/go-farmer/elasticsearch"
	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

const sqlfileBasename = "sqlite.db"

type flatDB struct {
	path        string
	conn        *sqlite.Conn
	transaction func(*error)
	closed      bool
}

func newFlatDB(dir string) (*flatDB, error) {
	err := os.MkdirAll(dir, dbDirPerms)
	if err != nil {
		return nil, err
	}

	path := filepath.Join(dir, sqlfileBasename)

	conn, err := prepareSQLiteDB(path)
	if err != nil {
		return nil, err
	}

	return &flatDB{
		path:        path,
		conn:        conn,
		transaction: sqlitex.Transaction(conn),
	}, nil
}

func prepareSQLiteDB(path string) (*sqlite.Conn, error) {
	conn, err := sqlite.OpenConn(path)
	if err != nil {
		return nil, err
	}

	err = sqlitex.ExecuteTransient(conn, `CREATE TABLE Hits (
	_id TEXT,
	timestamp INTEGER,
	ACCOUNTING_NAME TEXT,
	USER_NAME TEXT,
	IsGPU INTEGER,
	AVAIL_CPU_TIME_SEC INTEGER,
	Command TEXT,
	JOB_NAME TEXT,
	Job TEXT,
	MEM_REQUESTED_MB INTEGER,
	MEM_REQUESTED_MB_SEC INTEGER,
	NUM_EXEC_PROCS INTEGER,
	PENDING_TIME_SEC INTEGER,
	QUEUE_NAME TEXT,
	RUN_TIME_SEC INTEGER,
	WASTED_CPU_SECONDS REAL,
	WASTED_MB_SECONDS REAL
);`, nil)

	return conn, err
}

func (f *flatDB) Store(hit es.Hit) error {
	d := hit.Details

	isGPU := 0
	if strings.HasPrefix(d.QueueName, gpuPrefix) {
		isGPU = 1
	}

	const insert = "INSERT INTO Hits VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17);"

	err := sqlitex.ExecuteTransient(f.conn, insert, &sqlitex.ExecOptions{
		Args: []any{
			hit.ID, d.Timestamp, d.AccountingName, d.UserName, isGPU, d.AvailCPUTimeSec,
			d.Command, d.JobName, d.Job, d.MemRequestedMB, d.MemRequestedMBSec,
			d.NumExecProcs, d.PendingTimeSec, d.QueueName, d.RunTimeSec, d.WastedCPUSeconds,
			d.WastedMBSeconds,
		},
	})

	return err
}

func (f *flatDB) Close() error {
	if f.closed {
		return nil
	}

	var err error

	f.transaction(&err)

	err = f.createIndexes()

	f.closed = true

	if err != nil {
		f.conn.Close()

		return err
	}

	return f.conn.Close()
}

func (f *flatDB) createIndexes() error {
	for _, create := range []string{
		`CREATE INDEX timestamp on Hits (timestamp);`,
		`CREATE INDEX accounting_name on Hits (ACCOUNTING_NAME);`,
		`CREATE INDEX user_name on Hits (USER_NAME);`,
		`CREATE INDEX isgpu on Hits (IsGPU);`,
		`CREATE INDEX ta on Hits (timestamp, ACCOUNTING_NAME);`,
		`CREATE INDEX tag on Hits (timestamp, ACCOUNTING_NAME, IsGPU);`,
		`CREATE INDEX tau on Hits (timestamp, ACCOUNTING_NAME, USER_NAME);`,
		`CREATE INDEX taug on Hits (timestamp, ACCOUNTING_NAME, USER_NAME, IsGPU);`,
	} {
		err := sqlitex.ExecuteTransient(f.conn, create, nil)
		if err != nil {
			return err
		}
	}

	return nil
}

func scrollFlatFile(dbFilePath string, filter *sqlFilter, result *es.Result) error {
	conn, err := sqlite.OpenConn(dbFilePath, sqlite.OpenReadOnly)
	if err != nil {
		return err
	}

	defer conn.Close()

	return sqlitex.ExecuteTransient(conn, filter.toSelect(), &sqlitex.ExecOptions{
		ResultFunc: func(stmt *sqlite.Stmt) error {
			result.AddHitDetails(sqlResultToDetails(stmt, filter))

			return nil
		},
	})
}

func sqlResultToDetails(stmt *sqlite.Stmt, filter *sqlFilter) *es.Details {
	var d *es.Details

	if len(filter.fields) == 0 {
		d = getAllFieldsFromSQLResult(stmt)
	} else {
		d = getDesiredFieldsFromSQLResult(stmt, filter.fields)
	}

	d.BOM = filter.bom

	return d
}

func getAllFieldsFromSQLResult(stmt *sqlite.Stmt) *es.Details {
	return &es.Details{
		ID:                stmt.ColumnText(0),
		Timestamp:         stmt.ColumnInt64(1),
		AccountingName:    stmt.ColumnText(2),   //nolint:mnd
		UserName:          stmt.ColumnText(3),   //nolint:mnd
		AvailCPUTimeSec:   stmt.ColumnInt64(5),  //nolint:mnd
		Command:           stmt.ColumnText(6),   //nolint:mnd
		JobName:           stmt.ColumnText(7),   //nolint:mnd
		Job:               stmt.ColumnText(8),   //nolint:mnd
		MemRequestedMB:    stmt.ColumnInt64(9),  //nolint:mnd
		MemRequestedMBSec: stmt.ColumnInt64(10), //nolint:mnd
		NumExecProcs:      stmt.ColumnInt64(11), //nolint:mnd
		PendingTimeSec:    stmt.ColumnInt64(12), //nolint:mnd
		QueueName:         stmt.ColumnText(13),  //nolint:mnd
		RunTimeSec:        stmt.ColumnInt64(14), //nolint:mnd
		WastedCPUSeconds:  stmt.ColumnFloat(15), //nolint:mnd
		WastedMBSeconds:   stmt.ColumnFloat(16), //nolint:mnd
	}
}

func getDesiredFieldsFromSQLResult(stmt *sqlite.Stmt, fields []string) *es.Details { //nolint:funlen,gocyclo,cyclop
	d := &es.Details{
		ID: stmt.GetText("_id"),
	}

	for _, field := range fields {
		switch field {
		case "timestamp":
			d.Timestamp = stmt.GetInt64(field)
		case "ACCOUNTING_NAME":
			d.AccountingName = stmt.GetText(field)
		case "USER_NAME":
			d.UserName = stmt.GetText(field)
		case "AVAIL_CPU_TIME_SEC":
			d.AvailCPUTimeSec = stmt.GetInt64(field)
		case "Command":
			d.Command = stmt.GetText(field)
		case "JOB_NAME":
			d.JobName = stmt.GetText(field)
		case "Job":
			d.Job = stmt.GetText(field)
		case "MEM_REQUESTED_MB":
			d.MemRequestedMB = stmt.GetInt64(field)
		case "MEM_REQUESTED_MB_SEC":
			d.MemRequestedMBSec = stmt.GetInt64(field)
		case "NUM_EXEC_PROCS":
			d.NumExecProcs = stmt.GetInt64(field)
		case "PENDING_TIME_SEC":
			d.PendingTimeSec = stmt.GetInt64(field)
		case "QUEUE_NAME":
			d.QueueName = stmt.GetText(field)
		case "RUN_TIME_SEC":
			d.RunTimeSec = stmt.GetInt64(field)
		case "WASTED_CPU_SECONDS":
			d.WastedCPUSeconds = stmt.GetFloat(field)
		case "WASTED_MB_SECONDS":
			d.WastedMBSeconds = stmt.GetFloat(field)
		}
	}

	return d
}
