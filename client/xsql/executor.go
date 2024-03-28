package xsql

import (
	"context"
	"errors"
	"net/http"
	"reflect"
	"strings"

	"github.com/go-sql-driver/mysql"
	"github.com/reverted/ex"
)

type Logger interface {
	Infof(string, ...interface{})
}

type Tracer interface {
	StartSpan(context.Context, string, ...ex.SpanTag) (ex.Span, context.Context)
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

func WithTracer(tracer Tracer) opt {
	return func(self *executor) {
		self.Tracer = tracer
	}
}

func WithConnection(connection Connection) opt {
	return func(self *executor) {
		self.Connection = connection
	}
}

func NewExecutor(logger Logger, opts ...opt) *executor {

	executor := &executor{
		Logger:    logger,
		Tracer:    noopTracer{},
		Scanner:   NewScanner(),
		Formatter: NewMysqlFormatter(),
	}

	for _, opt := range opts {
		opt(executor)
	}

	// This calls dial so this should only get initialized if conn is nil
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
	Tracer
}

func (self *executor) Execute(ctx context.Context, req ex.Request, data interface{}) (bool, error) {
	err := self.execute(ctx, req, data)

	switch t := err.(type) {
	case *mysql.MySQLError:
		return (t.Number == 1213), err // retry on deadlock

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

	span, spanCtx := self.Tracer.StartSpan(ctx, "query")
	defer span.Finish()

	rows, err := self.queryContext(spanCtx, tx, stmt)
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

	span, spanCtx := self.Tracer.StartSpan(ctx, "delete")
	defer span.Finish()

	if data != nil {
		q := ex.Query(cmd.Resource, cmd.Where, cmd.Limit, cmd.Offset)
		if err := self.query(spanCtx, tx, q, data); err != nil {
			return err
		}
	}

	return self.stmt(spanCtx, tx, stmt)
}

func (self *executor) insert(ctx context.Context, tx Tx, cmd ex.Command, data interface{}) error {

	stmt, err := self.Formatter.Format(cmd)
	if err != nil {
		return err
	}

	self.Logger.Infof(">>> %v", stmt)

	span, spanCtx := self.Tracer.StartSpan(ctx, "insert")
	defer span.Finish()

	res, err := self.execContext(spanCtx, tx, stmt)
	if err != nil {
		return err
	}

	if data != nil {
		id, err := res.LastInsertId()
		if err != nil {
			return err
		}

		if id == 0 {
			return self.Scanner.Scan(emptyRows{}, data)
		}

		q := ex.Query(cmd.Resource, ex.Where{"id": id})
		return self.query(spanCtx, tx, q, data)
	}

	return nil
}

func (self *executor) update(ctx context.Context, tx Tx, cmd ex.Command, data interface{}) error {

	stmt, err := self.Formatter.Format(cmd)
	if err != nil {
		return err
	}

	self.Logger.Infof(">>> %v", stmt)

	span, spanCtx := self.Tracer.StartSpan(ctx, "update")
	defer span.Finish()

	if err := self.stmt(spanCtx, tx, stmt); err != nil {
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
		return self.query(spanCtx, tx, q, data)
	}

	return nil
}

func (self *executor) batch(ctx context.Context, tx Tx, batch ex.Batch, data interface{}) error {

	span, spanCtx := self.Tracer.StartSpan(ctx, "batch")
	defer span.Finish()

	for i, c := range batch.Requests {
		if i < len(batch.Requests)-1 {
			if err := self.executeTx(spanCtx, tx, c, nil); err != nil {
				return err
			}
		} else {
			if err := self.executeTx(spanCtx, tx, c, data); err != nil {
				return err
			}
		}
	}

	return nil
}

func (self *executor) stmt(ctx context.Context, tx Tx, stmt ex.Statement) error {

	span, spanCtx := self.Tracer.StartSpan(ctx, "stmt")
	defer span.Finish()

	_, err := self.execContext(spanCtx, tx, stmt)
	return err
}

func (self *executor) queryContext(ctx context.Context, tx Tx, stmt ex.Statement) (Rows, error) {

	span, spanCtx := self.Tracer.StartSpan(ctx, "exec", ex.SpanTag{"stmt", stmt.Stmt})
	defer span.Finish()

	return tx.QueryContext(spanCtx, stmt.Stmt, stmt.Args...)
}

func (self *executor) execContext(ctx context.Context, tx Tx, stmt ex.Statement) (Result, error) {

	span, spanCtx := self.Tracer.StartSpan(ctx, "exec", ex.SpanTag{"stmt", stmt.Stmt})
	defer span.Finish()

	return tx.ExecContext(spanCtx, stmt.Stmt, stmt.Args...)
}

type noopSpan struct{}

func (self noopSpan) Finish() {}

type noopTracer struct{}

func (self noopTracer) StartSpan(ctx context.Context, name string, tags ...ex.SpanTag) (ex.Span, context.Context) {
	return noopSpan{}, ctx
}

func (self noopTracer) InjectSpan(ctx context.Context, r *http.Request) {
}

func (self noopTracer) ExtractSpan(r *http.Request, name string) (ex.Span, context.Context) {
	return noopSpan{}, r.Context()
}
