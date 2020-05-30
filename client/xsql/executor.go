package xsql

import (
	"context"
	"errors"
	"reflect"
	"strings"

	"github.com/go-sql-driver/mysql"
	"github.com/reverted/ex"
)

type Logger interface {
	Infof(string, ...interface{})
}

type Formatter interface {
	Format(ex.Command) (ex.Statement, error)
}

type Scanner interface {
	Scan(Rows, interface{}) error
}

type Connection interface {
	Begin() (Tx, error)
}

type Tx interface {
	Rollback() error
	QueryContext(context.Context, string, ...interface{}) (Rows, error)
	Query(string, ...interface{}) (Rows, error)
	ExecContext(context.Context, string, ...interface{}) (Result, error)
	Exec(string, ...interface{}) (Result, error)
	Commit() error
}

type Rows interface {
	Err() error
	Next() bool
	ColumnTypes() ([]ColumnType, error)
	Scan(...interface{}) error
	Close() error
}

type ColumnType interface {
	Name() string
	ScanType() reflect.Type
	DatabaseTypeName() string
}

type Result interface {
	LastInsertId() (int64, error)
}

type opt func(*executor)

func WithMysqlFormatter() opt {
	return func(self *executor) {
		self.Formatter = NewMysqlFormatter()
	}
}

func WithFormatter(formatter Formatter) opt {
	return func(self *executor) {
		self.Formatter = formatter
	}
}

func WithScanner(scanner Scanner) opt {
	return func(self *executor) {
		self.Scanner = scanner
	}
}

func WithConnection(connection Connection) opt {
	return func(self *executor) {
		self.Connection = connection
	}
}

func NewExecutor(logger Logger, opts ...opt) *executor {

	executor := &executor{Logger: logger}

	for _, opt := range opts {
		opt(executor)
	}

	if executor.Scanner == nil {
		WithScanner(NewScanner())(executor)
	}

	if executor.Formatter == nil {
		WithMysqlFormatter()(executor)
	}

	if executor.Connection == nil {
		WithConnection(NewConn("mysql", "tcp(localhost:3306)/dev"))(executor)
	}

	return executor
}

type executor struct {
	Logger
	Formatter
	Connection
	Scanner
}

func (self *executor) Execute(ctx context.Context, req ex.Request, data interface{}) (bool, error) {
	err := self.execute(ctx, req, data)

	switch t := err.(type) {
	case *mysql.MySQLError:
		return (t.Number == 1213), err

	default:
		return false, err
	}
}

func (self *executor) execute(ctx context.Context, req ex.Request, data interface{}) error {

	tx, err := self.Connection.Begin()
	if err != nil {
		return err
	}

	defer tx.Rollback()

	if err = self.executeTx(ctx, tx, req, data); err != nil {
		return err
	}

	return tx.Commit()
}

func (self *executor) executeTx(ctx context.Context, tx Tx, req ex.Request, data interface{}) error {

	switch c := req.(type) {
	case ex.Statement:
		return self.stmt(ctx, tx, c)

	case ex.Command:
		return self.cmd(ctx, tx, c, data)

	case ex.Batch:
		return self.batch(ctx, tx, c, data)

	default:
		return errors.New("Unsupported req")
	}
}

func (self *executor) cmd(ctx context.Context, tx Tx, cmd ex.Command, data interface{}) error {

	switch strings.ToUpper(cmd.Action) {
	case "QUERY":
		return self.query(ctx, tx, cmd, data)

	case "DELETE":
		return self.delete(ctx, tx, cmd, data)

	case "INSERT":
		return self.insert(ctx, tx, cmd, data)

	case "UPDATE":
		return self.update(ctx, tx, cmd, data)

	default:
		return errors.New("Unsupported cmd")
	}
}

func (self *executor) query(ctx context.Context, tx Tx, cmd ex.Command, data interface{}) error {

	stmt, err := self.Formatter.Format(cmd)
	if err != nil {
		return err
	}

	self.Logger.Infof(">>> %v", stmt)

	rows, err := self.queryContext(ctx, tx, stmt)
	if err != nil {
		return err
	}

	defer rows.Close()

	return self.Scanner.Scan(rows, data)
}

func (self *executor) delete(ctx context.Context, tx Tx, cmd ex.Command, data interface{}) error {

	stmt, err := self.Formatter.Format(cmd)
	if err != nil {
		return err
	}

	self.Logger.Infof(">>> %v", stmt)

	if data != nil {
		q := ex.Query(cmd.Resource, cmd.Where, cmd.Limit, cmd.Offset)
		if err := self.query(ctx, tx, q, data); err != nil {
			return err
		}
	}

	return self.stmt(ctx, tx, stmt)
}

func (self *executor) insert(ctx context.Context, tx Tx, cmd ex.Command, data interface{}) error {

	stmt, err := self.Formatter.Format(cmd)
	if err != nil {
		return err
	}

	self.Logger.Infof(">>> %v", stmt)

	res, err := self.execContext(ctx, tx, stmt)
	if err != nil {
		return err
	}

	id, err := res.LastInsertId()
	if err != nil {
		return err
	}

	if data != nil {
		if id > 0 {
			q := ex.Query(cmd.Resource, ex.Where{"id": id})
			return self.query(ctx, tx, q, data)
		}
	}

	return nil
}

func (self *executor) update(ctx context.Context, tx Tx, cmd ex.Command, data interface{}) error {

	stmt, err := self.Formatter.Format(cmd)
	if err != nil {
		return err
	}

	self.Logger.Infof(">>> %v", stmt)

	if err := self.stmt(ctx, tx, stmt); err != nil {
		return err
	}

	if data != nil {
		where := cmd.Where
		for key, _ := range cmd.Where {
			if updated, ok := cmd.Values[key]; ok {
				where[key] = updated
			}
		}

		q := ex.Query(cmd.Resource, where, cmd.Limit, cmd.Offset)
		return self.query(ctx, tx, q, data)
	}

	return nil
}

func (self *executor) batch(ctx context.Context, tx Tx, batch ex.Batch, data interface{}) error {

	for _, c := range batch.Requests {
		if err := self.executeTx(ctx, tx, c, data); err != nil {
			return err
		}
	}

	return nil
}

func (self *executor) stmt(ctx context.Context, tx Tx, stmt ex.Statement) error {
	_, err := self.execContext(ctx, tx, stmt)
	return err
}

func (self *executor) queryContext(ctx context.Context, tx Tx, stmt ex.Statement) (Rows, error) {
	if ctx != nil {
		return tx.QueryContext(ctx, stmt.Stmt, stmt.Args...)
	} else {
		return tx.Query(stmt.Stmt, stmt.Args...)
	}
}

func (self *executor) execContext(ctx context.Context, tx Tx, stmt ex.Statement) (Result, error) {
	if ctx != nil {
		return tx.ExecContext(ctx, stmt.Stmt, stmt.Args...)
	} else {
		return tx.Exec(stmt.Stmt, stmt.Args...)
	}
}
