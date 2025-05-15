package xsql

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"sync"

	"github.com/go-sql-driver/mysql"
	"github.com/reverted/ex"
	"github.com/reverted/ex/client/xsql/xmysql"
	"github.com/reverted/ex/client/xsql/xpg"
)

type Logger interface {
	Infof(string, ...any)
}

type Tracer interface {
	StartSpan(context.Context, string, ...ex.SpanTag) (ex.Span, context.Context)
}

type Formatter interface {
	Format(ex.Command, map[string]string) (ex.Statement, error)
}

type Scanner interface {
	Scan(Rows, any) error
}

type Connection interface {
	Begin() (Tx, error)
}

type Tx interface {
	Rollback() error
	QueryContext(context.Context, string, ...any) (Rows, error)
	Query(string, ...any) (Rows, error)
	ExecContext(context.Context, string, ...any) (Result, error)
	Exec(string, ...any) (Result, error)
	Commit() error
}

type Rows interface {
	Err() error
	Next() bool
	ColumnTypes() ([]ColumnType, error)
	Scan(...any) error
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

func WithPostgresFormatter() opt {
	return func(e *executor) {
		e.Formatter = xpg.NewFormatter()
	}
}

func WithMysqlFormatter() opt {
	return func(e *executor) {
		e.Formatter = xmysql.NewFormatter()
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
		Formatter: xmysql.NewFormatter(),
		TypeCache: TypeCache{},
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
	sync.Mutex

	Logger
	Formatter
	Connection
	Scanner
	Tracer

	TypeCache TypeCache
}

func (e *executor) Execute(ctx context.Context, req ex.Request, data any) (bool, error) {
	err := e.execute(ctx, req, data)

	switch t := err.(type) {
	case *mysql.MySQLError:
		return (t.Number == 1213), err // retry on deadlock

	default:
		return false, err
	}
}

func (e *executor) execute(ctx context.Context, req ex.Request, data any) error {

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

func (e *executor) executeTx(ctx context.Context, tx Tx, req ex.Request, data any) error {

	switch c := req.(type) {
	case ex.Instruction:
		return e.stmt(ctx, tx, ex.Statement{Stmt: c.Stmt}, nil)

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

func (e *executor) cmd(ctx context.Context, tx Tx, cmd ex.Command, data any) error {

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

func (e *executor) query(ctx context.Context, tx Tx, cmd ex.Command, data any) error {

	types, err := e.getColumnTypes(ctx, tx, cmd.Resource)
	if err != nil {
		return err
	}

	stmt, err := e.Formatter.Format(cmd, types)
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

func (e *executor) delete(ctx context.Context, tx Tx, cmd ex.Command, data any) error {

	types, err := e.getColumnTypes(ctx, tx, cmd.Resource)
	if err != nil {
		return err
	}

	stmt, err := e.Formatter.Format(cmd, types)
	if err != nil {
		return err
	}

	e.Logger.Infof(">>> %v", stmt)

	span, spanCtx := e.Tracer.StartSpan(ctx, "delete")
	defer span.Finish()

	if data != nil {
		q := ex.Query(cmd.Resource, cmd.Where, cmd.LimitConfig, cmd.OffsetConfig)
		if err := e.query(spanCtx, tx, q, data); err != nil {
			return err
		}
	}

	return e.stmt(spanCtx, tx, stmt, nil)
}

func (e *executor) insert(ctx context.Context, tx Tx, cmd ex.Command, data any) error {

	types, err := e.getColumnTypes(ctx, tx, cmd.Resource)
	if err != nil {
		return err
	}

	stmt, err := e.Formatter.Format(cmd, types)
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
			return e.Scanner.Scan(emptyRows{}, data)
		}

		if id == 0 {
			return e.Scanner.Scan(emptyRows{}, data)
		}

		q := ex.Query(cmd.Resource, ex.Where{"id": id})
		return e.query(spanCtx, tx, q, data)
	}

	return nil
}

func (e *executor) update(ctx context.Context, tx Tx, cmd ex.Command, data any) error {

	types, err := e.getColumnTypes(ctx, tx, cmd.Resource)
	if err != nil {
		return err
	}

	stmt, err := e.Formatter.Format(cmd, types)
	if err != nil {
		return err
	}

	e.Logger.Infof(">>> %v", stmt)

	span, spanCtx := e.Tracer.StartSpan(ctx, "update")
	defer span.Finish()

	if err := e.stmt(spanCtx, tx, stmt, nil); err != nil {
		return err
	}

	if data != nil {
		where := cmd.Where
		for key := range cmd.Where {
			if updated, ok := cmd.Values[key]; ok {
				where[key] = updated
			}
		}

		q := ex.Query(cmd.Resource, where, cmd.LimitConfig, cmd.OffsetConfig)
		return e.query(spanCtx, tx, q, data)
	}

	return nil
}

func (e *executor) batch(ctx context.Context, tx Tx, batch ex.Batch, data any) error {

	span, spanCtx := e.Tracer.StartSpan(ctx, "batch")
	defer span.Finish()

	var indexOfLastNonInstruction int
	for i, r := range batch.Requests {
		if _, ok := r.(ex.Instruction); ok {
			continue
		}
		indexOfLastNonInstruction = i
	}

	for i, r := range batch.Requests {

		if i == indexOfLastNonInstruction { // only attempt parsing 'data' here
			if err := e.executeTx(spanCtx, tx, r, data); err != nil {
				return err
			}
		} else {
			if err := e.executeTx(spanCtx, tx, r, nil); err != nil {
				return err
			}
		}
	}

	return nil
}

func (e *executor) stmt(ctx context.Context, tx Tx, stmt ex.Statement, data any) error {

	span, spanCtx := e.Tracer.StartSpan(ctx, "stmt")
	defer span.Finish()

	if data == nil {
		_, err := e.execContext(spanCtx, tx, stmt)
		return err
	}

	rows, err := e.queryContext(spanCtx, tx, stmt)
	if err != nil {
		return err
	}

	defer rows.Close()

	return e.Scanner.Scan(rows, data)
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

func (e *executor) getColumnTypes(ctx context.Context, tx Tx, tableName string) (TableTypes, error) {
	e.Lock()
	defer e.Unlock()

	if types, ok := e.TypeCache[tableName]; ok && len(types) > 0 {
		return types, nil
	}

	query := fmt.Sprintf("SELECT * FROM %s LIMIT 0", tableName)
	rows, err := e.queryContext(ctx, tx, ex.Statement{Stmt: query})
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	columns := TableTypes{}
	for _, col := range columnTypes {
		columns[col.Name()] = col.DatabaseTypeName()
	}

	if len(columns) > 0 {
		e.TypeCache[tableName] = columns
	}

	return columns, nil
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

type TypeCache map[string]TableTypes

type TableTypes map[string]string
