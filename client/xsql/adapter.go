package xsql

import (
	"context"
	"database/sql"
	"log"
	"time"
)

func NewConn(driver, uri string) *conn {

	var (
		err error
		db  *sql.DB
	)

	for _, interval := range []int{0, 1, 2, 5, 10, 30, 60} {
		time.Sleep(time.Duration(interval) * time.Second)

		if db, err = sql.Open(driver, uri); err != nil {
			continue
		}

		if err := db.Ping(); err != nil {
			continue
		}

		break
	}

	if err != nil {
		log.Fatal(err)
	}

	return &conn{db}
}

type conn struct {
	*sql.DB
}

func (self *conn) Begin() (Tx, error) {
	t, err := self.DB.Begin()
	if err != nil {
		return nil, err
	}
	return &tx{t}, nil
}

type tx struct {
	*sql.Tx
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

type rows struct {
	*sql.Rows
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

type result struct {
	sql.Result
}

type emptyRows struct{}

func (self emptyRows) Err() error {
	return nil
}

func (self emptyRows) Next() bool {
	return false
}

func (self emptyRows) ColumnTypes() ([]ColumnType, error) {
	return nil, nil
}

func (self emptyRows) Scan(...interface{}) error {
	return nil
}

func (self emptyRows) Close() error {
	return nil
}
