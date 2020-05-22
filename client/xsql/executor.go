package xsql

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"reflect"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/reverted/ex"
)

type Logger interface {
	Fatal(...interface{})
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
	Close() error
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

func FromEnv() opt {
	return func(self *executor) {
		WithMysql(os.Getenv("REVERTED_MYSQL_URL"))(self)
	}
}

func WithMysql(uri string) opt {
	return func(self *executor) {
		WithFormatter(NewMysqlFormatter())(self)
		WithMysqlConn(uri)(self)
	}
}

func WithMysqlConn(uri string) opt {
	return func(self *executor) {

		var (
			err  error
			conn *sql.DB
		)

		for _, interval := range []int{0, 1, 2, 5, 10, 30, 60} {
			time.Sleep(time.Duration(interval) * time.Second)

			if conn, err = sql.Open("mysql", uri); err != nil {
				continue
			}

			if err := conn.Ping(); err != nil {
				continue
			}

			break
		}

		if err != nil {
			self.Logger.Fatal(err)
		}

		WithConnection(NewSqlAdapter(conn))(self)
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

func NewExecutorFromEnv(logger Logger) *executor {
	return NewExecutor(logger, FromEnv())
}

func NewExecutor(logger Logger, opts ...opt) *executor {

	executor := &executor{Logger: logger}

	for _, opt := range opts {
		opt(executor)
	}

	if executor.Formatter == nil {
		WithFormatter(NewMysqlFormatter())(executor)
	}

	if executor.Scanner == nil {
		WithScanner(NewScanner())(executor)
	}

	if executor.Connection == nil {
		WithMysqlConn("tcp(localhost:3306)/dev")(executor)
	}

	return executor
}

type executor struct {
	Logger
	Formatter
	Scanner
	Connection
}

func (self *executor) Close() error {
	return self.Connection.Close()
}

func (self *executor) Execute(req ex.Request, data interface{}) (bool, error) {
	err := self.execute(req, data)

	switch t := err.(type) {
	case *mysql.MySQLError:
		return (t.Number == 1213), err

	default:
		return false, err
	}
}

func (self *executor) execute(req ex.Request, data interface{}) error {

	tx, err := self.Connection.Begin()
	if err != nil {
		return err
	}

	defer tx.Rollback()

	if err = self.executeTx(tx, req, data); err != nil {
		return err
	}

	return tx.Commit()
}

func (self *executor) executeTx(tx Tx, req ex.Request, data interface{}) error {

	switch c := req.(type) {
	case ex.Statement:
		return self.stmt(tx, c)

	case ex.Command:
		return self.cmd(tx, c, data)

	case ex.Batch:
		return self.batch(tx, c, data)

	default:
		return errors.New("Unsupported req")
	}
}

func (self *executor) cmd(tx Tx, cmd ex.Command, data interface{}) error {

	switch strings.ToUpper(cmd.Action) {
	case "QUERY":
		return self.query(tx, cmd, data)

	case "DELETE":
		return self.delete(tx, cmd, data)

	case "INSERT":
		return self.insert(tx, cmd, data)

	case "UPDATE":
		return self.update(tx, cmd, data)

	default:
		return errors.New("Unsupported cmd")
	}
}

func (self *executor) query(tx Tx, cmd ex.Command, data interface{}) error {

	stmt, err := self.Formatter.Format(cmd)
	if err != nil {
		return err
	}

	self.Logger.Infof(">>> %v", stmt)

	rows, err := self.queryContext(tx, stmt)
	if err != nil {
		return err
	}

	defer rows.Close()

	return self.Scanner.Scan(rows, data)
}

func (self *executor) delete(tx Tx, cmd ex.Command, data interface{}) error {

	stmt, err := self.Formatter.Format(cmd)
	if err != nil {
		return err
	}

	self.Logger.Infof(">>> %v", stmt)

	if data != nil {
		q := ex.Query(cmd.Resource, cmd.Where, cmd.Limit, cmd.Offset)
		if err := self.query(tx, q.WithContext(cmd.Context), data); err != nil {
			return err
		}
	}

	return self.stmt(tx, stmt)
}

func (self *executor) insert(tx Tx, cmd ex.Command, data interface{}) error {

	stmt, err := self.Formatter.Format(cmd)
	if err != nil {
		return err
	}

	self.Logger.Infof(">>> %v", stmt)

	res, err := self.execContext(tx, stmt)
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
			return self.query(tx, q.WithContext(cmd.Context), data)
		}
	}

	return nil
}

func (self *executor) update(tx Tx, cmd ex.Command, data interface{}) error {

	stmt, err := self.Formatter.Format(cmd)
	if err != nil {
		return err
	}

	self.Logger.Infof(">>> %v", stmt)

	if err := self.stmt(tx, stmt); err != nil {
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
		return self.query(tx, q.WithContext(cmd.Context), data)
	}

	return nil
}

func (self *executor) batch(tx Tx, batch ex.Batch, data interface{}) error {

	for _, c := range batch.Requests {
		if err := self.executeTx(tx, c, data); err != nil {
			return err
		}
	}

	return nil
}

func (self *executor) stmt(tx Tx, stmt ex.Statement) error {
	_, err := self.execContext(tx, stmt)
	return err
}

func (self *executor) queryContext(tx Tx, stmt ex.Statement) (Rows, error) {
	if stmt.Context != nil {
		return tx.QueryContext(stmt.Context, stmt.Stmt, stmt.Args...)
	} else {
		return tx.Query(stmt.Stmt, stmt.Args...)
	}
}

func (self *executor) execContext(tx Tx, stmt ex.Statement) (Result, error) {
	if stmt.Context != nil {
		return tx.ExecContext(stmt.Context, stmt.Stmt, stmt.Args...)
	} else {
		return tx.Exec(stmt.Stmt, stmt.Args...)
	}
}
