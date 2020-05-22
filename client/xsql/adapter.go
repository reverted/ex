package xsql

import (
	"context"
	"database/sql"
)

func NewSqlAdapter(db *sql.DB) *adapter {
	return &adapter{db}
}

type adapter struct {
	*sql.DB
}

func (self *adapter) Begin() (Tx, error) {
	t, err := self.DB.Begin()
	if err != nil {
		return nil, err
	}
	return &tx{t}, nil
}

func (self *adapter) Close() error {
	return self.DB.Close()
}

type tx struct {
	*sql.Tx
}

func (self *tx) Rollback() error {
	return self.Tx.Rollback()
}

func (self *tx) QueryContext(ctx context.Context, stmt string, args ...interface{}) (Rows, error) {
	r, err := self.Tx.QueryContext(ctx, stmt, args...)
	if err != nil {
		return nil, err
	}
	return &rows{r}, nil
}

func (self *tx) Query(stmt string, args ...interface{}) (Rows, error) {
	r, err := self.Tx.Query(stmt, args...)
	if err != nil {
		return nil, err
	}
	return &rows{r}, nil
}

func (self *tx) ExecContext(ctx context.Context, stmt string, args ...interface{}) (Result, error) {
	r, err := self.Tx.ExecContext(ctx, stmt, args...)
	if err != nil {
		return nil, err
	}
	return &result{r}, nil
}

func (self *tx) Exec(stmt string, args ...interface{}) (Result, error) {
	r, err := self.Tx.Exec(stmt, args...)
	if err != nil {
		return nil, err
	}
	return &result{r}, nil
}

func (self *tx) Commit() error {
	return self.Tx.Commit()
}

type rows struct {
	*sql.Rows
}

func (self *rows) Err() error {
	return self.Rows.Err()
}

func (self *rows) Next() bool {
	return self.Rows.Next()
}

func (self *rows) ColumnTypes() ([]ColumnType, error) {
	var types []ColumnType

	ts, err := self.Rows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	for _, t := range ts {
		types = append(types, t)
	}

	return types, nil
}

func (self *rows) Scan(dest ...interface{}) error {
	return self.Rows.Scan(dest...)
}

func (self *rows) Close() error {
	return self.Rows.Close()
}

type result struct {
	sql.Result
}

func (self *result) LastInsertId() (int64, error) {
	return self.Result.LastInsertId()
}
