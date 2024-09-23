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
	return func(e *executor) {
		e.Formatter = NewMysqlFormatter()
	}
}

func WithFormatter(formatter Formatter) opt {
	return func(e *executor) {
		e.Formatter = formatter
	}
}

func WithScanner(scanner Scanner) opt {
	return func(e *executor) {
		e.Scanner = scanner
	}
}

func WithTracer(tracer Tracer) opt {
	return func(e *executor) {
		e.Tracer = tracer
	}
}

func WithConnection(connection Connection) opt {
	return func(e *executor) {
		e.Connection = connection
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

func (e *executor) Execute(ctx context.Context, req ex.Request, data interface{}) (bool, error) {
	err := e.execute(ctx, req, data)

	switch t := err.(type) {
	case *mysql.MySQLError:
		return (t.Number == 1213), err // retry on deadlock

	default:
		return false, err
	}
}

func (e *executor) execute(ctx context.Context, req ex.Request, data interface{}) error {

	tx, err := e.Connection.Begin()
	if err != nil {
		return err
	}

	defer tx.Rollback()

	if err = e.executeTx(ctx, tx, req, data); err != nil {
		return err
	}

	return tx.Commit()
}

func (e *executor) executeTx(ctx context.Context, tx Tx, req ex.Request, data interface{}) error {

	switch c := req.(type) {
	case ex.Statement:
		return e.stmt(ctx, tx, c, data)

	case ex.Command:
		return e.cmd(ctx, tx, c, data)

	case ex.Batch:
		return e.batch(ctx, tx, c, data)

	default:
		return errors.New("unsupported req")
	}
}

func (e *executor) cmd(ctx context.Context, tx Tx, cmd ex.Command, data interface{}) error {

	switch strings.ToUpper(cmd.Action) {
	case "QUERY":
		return e.query(ctx, tx, cmd, data)

	case "DELETE":
		return e.delete(ctx, tx, cmd, data)

	case "INSERT":
		return e.insert(ctx, tx, cmd, data)

	case "UPDATE":
		return e.update(ctx, tx, cmd, data)

	default:
		return errors.New("unsupported cmd")
	}
}

func (e *executor) query(ctx context.Context, tx Tx, cmd ex.Command, data interface{}) error {

	stmt, err := e.Formatter.Format(cmd)
	if err != nil {
		return err
	}

	e.Logger.Infof(">>> %v", stmt)

	span, spanCtx := e.Tracer.StartSpan(ctx, "query")
	defer span.Finish()

	rows, err := e.queryContext(spanCtx, tx, stmt)
	if err != nil {
		return err
	}

	defer rows.Close()

	return e.Scanner.Scan(rows, data)
}

func (e *executor) delete(ctx context.Context, tx Tx, cmd ex.Command, data interface{}) error {

	stmt, err := e.Formatter.Format(cmd)
	if err != nil {
		return err
	}

	e.Logger.Infof(">>> %v", stmt)

	span, spanCtx := e.Tracer.StartSpan(ctx, "delete")
	defer span.Finish()

	if data != nil {
		q := ex.Query(cmd.Resource, cmd.Where, cmd.Limit, cmd.Offset)
		if err := e.query(spanCtx, tx, q, data); err != nil {
			return err
		}
	}

	return e.stmt(spanCtx, tx, stmt, data)
}

func (e *executor) insert(ctx context.Context, tx Tx, cmd ex.Command, data interface{}) error {

	stmt, err := e.Formatter.Format(cmd)
	if err != nil {
		return err
	}

	e.Logger.Infof(">>> %v", stmt)

	span, spanCtx := e.Tracer.StartSpan(ctx, "insert")
	defer span.Finish()

	res, err := e.execContext(spanCtx, tx, stmt)
	if err != nil {
		return err
	}

	if data != nil {
		id, err := res.LastInsertId()
		if err != nil {
			return err
		}

		if id == 0 {
			return e.Scanner.Scan(emptyRows{}, data)
		}

		q := ex.Query(cmd.Resource, ex.Where{"id": id})
		return e.query(spanCtx, tx, q, data)
	}

	return nil
}

func (e *executor) update(ctx context.Context, tx Tx, cmd ex.Command, data interface{}) error {

	stmt, err := e.Formatter.Format(cmd)
	if err != nil {
		return err
	}

	e.Logger.Infof(">>> %v", stmt)

	span, spanCtx := e.Tracer.StartSpan(ctx, "update")
	defer span.Finish()

	if err := e.stmt(spanCtx, tx, stmt, data); err != nil {
		return err
	}

	if data != nil {
		where := cmd.Where
		for key := range cmd.Where {
			if updated, ok := cmd.Values[key]; ok {
				where[key] = updated
			}
		}

		q := ex.Query(cmd.Resource, where, cmd.Limit, cmd.Offset)
		return e.query(spanCtx, tx, q, data)
	}

	return nil
}

func (e *executor) batch(ctx context.Context, tx Tx, batch ex.Batch, data interface{}) error {

	span, spanCtx := e.Tracer.StartSpan(ctx, "batch")
	defer span.Finish()

	for i, c := range batch.Requests {
		if i < len(batch.Requests)-1 {
			if err := e.executeTx(spanCtx, tx, c, nil); err != nil {
				return err
			}
		} else {
			if err := e.executeTx(spanCtx, tx, c, data); err != nil {
				return err
			}
		}
	}

	return nil
}

func (e *executor) stmt(ctx context.Context, tx Tx, stmt ex.Statement, data interface{}) error {

	span, spanCtx := e.Tracer.StartSpan(ctx, "stmt")
	defer span.Finish()

	compare := strings.TrimSpace(strings.ToUpper(stmt.Stmt))
	isSelect := strings.HasPrefix(compare, "SELECT")

	if isSelect {
		rows, err := e.queryContext(spanCtx, tx, stmt)
		if err != nil {
			return err
		}

		defer rows.Close()

		return e.Scanner.Scan(rows, data)
	}

	_, err := e.execContext(spanCtx, tx, stmt)
	return err
}

func (e *executor) queryContext(ctx context.Context, tx Tx, stmt ex.Statement) (Rows, error) {

	span, spanCtx := e.Tracer.StartSpan(ctx, "exec", ex.SpanTag{Key: "stmt", Value: stmt.Stmt})
	defer span.Finish()

	return tx.QueryContext(spanCtx, stmt.Stmt, stmt.Args...)
}

func (e *executor) execContext(ctx context.Context, tx Tx, stmt ex.Statement) (Result, error) {

	span, spanCtx := e.Tracer.StartSpan(ctx, "exec", ex.SpanTag{Key: "stmt", Value: stmt.Stmt})
	defer span.Finish()

	return tx.ExecContext(spanCtx, stmt.Stmt, stmt.Args...)
}

type noopSpan struct{}

func (s noopSpan) Finish() {}

type noopTracer struct{}

func (t noopTracer) StartSpan(ctx context.Context, name string, tags ...ex.SpanTag) (ex.Span, context.Context) {
	return noopSpan{}, ctx
}

func (t noopTracer) InjectSpan(ctx context.Context, r *http.Request) {
}

func (t noopTracer) ExtractSpan(r *http.Request, name string) (ex.Span, context.Context) {
	return noopSpan{}, r.Context()
}
