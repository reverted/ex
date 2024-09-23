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

func (c *conn) Begin() (Tx, error) {
	t, err := c.DB.Begin()
	if err != nil {
		return nil, err
	}
	return &tx{t}, nil
}

type tx struct {
	*sql.Tx
}

func (t *tx) QueryContext(ctx context.Context, stmt string, args ...interface{}) (Rows, error) {
	r, err := t.Tx.QueryContext(ctx, stmt, args...)
	if err != nil {
		return nil, err
	}
	return &rows{r}, nil
}

func (t *tx) Query(stmt string, args ...interface{}) (Rows, error) {
	r, err := t.Tx.Query(stmt, args...)
	if err != nil {
		return nil, err
	}
	return &rows{r}, nil
}

func (t *tx) ExecContext(ctx context.Context, stmt string, args ...interface{}) (Result, error) {
	r, err := t.Tx.ExecContext(ctx, stmt, args...)
	if err != nil {
		return nil, err
	}
	return &result{r}, nil
}

func (t *tx) Exec(stmt string, args ...interface{}) (Result, error) {
	r, err := t.Tx.Exec(stmt, args...)
	if err != nil {
		return nil, err
	}
	return &result{r}, nil
}

type rows struct {
	*sql.Rows
}

func (r *rows) ColumnTypes() ([]ColumnType, error) {
	var types []ColumnType

	ts, err := r.Rows.ColumnTypes()
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

func (e emptyRows) Err() error {
	return nil
}

func (e emptyRows) Next() bool {
	return false
}

func (e emptyRows) ColumnTypes() ([]ColumnType, error) {
	return nil, nil
}

func (e emptyRows) Scan(...interface{}) error {
	return nil
}

func (e emptyRows) Close() error {
	return nil
}
