package sql

import (
	"database/sql"
	"errors"
	"os"
	"reflect"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/reverted/ex"
)

type Formatter interface {
	FormatQuery(ex.Command) ex.Statement
	FormatDelete(ex.Command) ex.Statement
	FormatInsert(ex.Command) ex.Statement
	FormatUpdate(ex.Command) ex.Statement
}

type Scanner interface {
	Scan(rows *sql.Rows, t reflect.Type) (interface{}, error)
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

		for _, interval := range []int{0, 1, 2, 5} {
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

		WithConn(conn)(self)
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

func WithConn(conn *sql.DB) opt {
	return func(self *executor) {
		self.DB = conn
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

	if executor.DB == nil {
		WithMysqlConn("tcp(localhost:3306)/dev")(executor)
	}

	return executor
}

type executor struct {
	Logger
	Formatter
	Scanner
	*sql.DB
}

func (self *executor) Close() error {
	return self.DB.Close()
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

	tx, err := self.DB.Begin()
	if err != nil {
		return err
	}

	defer tx.Rollback()

	if err = self.executeTx(tx, req, data); err != nil {
		return err
	}

	return tx.Commit()
}

func (self *executor) executeTx(tx *sql.Tx, req ex.Request, data interface{}) error {

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

func (self *executor) cmd(tx *sql.Tx, cmd ex.Command, data interface{}) error {

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

func (self *executor) delete(tx *sql.Tx, cmd ex.Command, data interface{}) error {

	stmt := self.Formatter.FormatDelete(cmd)

	self.Logger.Infof(">>> %v", stmt)

	if data != nil {
		if err := self.query(tx, cmd, data); err != nil {
			return err
		}
	}

	return self.stmt(tx, stmt)
}

func (self *executor) insert(tx *sql.Tx, cmd ex.Command, data interface{}) error {

	stmt := self.Formatter.FormatInsert(cmd)

	self.Logger.Infof(">>> %v", stmt)

	res, err := tx.Exec(stmt.Stmt, stmt.Args...)
	if err != nil {
		return err
	}

	id, err := res.LastInsertId()
	if err != nil {
		return err
	}

	if id > 0 && data != nil {
		query := ex.Query(cmd.Resource, ex.Where{"id": id})
		return self.query(tx, query, data)
	}

	return nil
}

func (self *executor) update(tx *sql.Tx, cmd ex.Command, data interface{}) error {

	stmt := self.Formatter.FormatUpdate(cmd)

	self.Logger.Infof(">>> %v", stmt)

	if err := self.stmt(tx, stmt); err != nil {
		return err
	}

	if data != nil {
		for key, _ := range cmd.Where {
			if updated, ok := cmd.Values[key]; ok {
				cmd.Where[key] = updated
			}
		}

		return self.query(tx, cmd, data)
	}

	return nil
}

func (self *executor) query(tx *sql.Tx, cmd ex.Command, data interface{}) error {

	stmt := self.Formatter.FormatQuery(cmd)

	self.Logger.Infof(">>> %v", stmt)

	rows, err := tx.Query(stmt.Stmt, stmt.Args...)
	if err != nil {
		return err
	}

	defer rows.Close()

	t := reflect.TypeOf(data)
	v := reflect.ValueOf(data)

	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	for v.Kind() == reflect.Ptr {
		if v.IsNil() {
			break
		}
		v = v.Elem()
	}

	if t.Kind() == reflect.Slice || t.Kind() == reflect.Array {
		return self.queryRowsTx(tx, rows, t, v)
	} else {
		return self.queryRowTx(tx, rows, t, v)
	}
}

func (self *executor) queryRowTx(tx *sql.Tx, rows *sql.Rows, t reflect.Type, v reflect.Value) error {

	if rows.Next() {
		instance, err := self.Scanner.Scan(rows, t)
		if err != nil {
			return err
		}

		val := reflect.ValueOf(instance)

		if t.Kind() != reflect.Ptr {
			val = reflect.Indirect(val)
		}

		if v.Kind() == reflect.Ptr {
			v.Set(val.Addr())
		} else {
			v.Set(val)
		}
	}

	if err := rows.Err(); err != nil {
		return err
	}

	if v.Kind() == reflect.Ptr && v.IsNil() {
		return errors.New("Not found")
	}

	return nil
}

func (self *executor) queryRowsTx(tx *sql.Tx, rows *sql.Rows, t reflect.Type, v reflect.Value) error {

	e := t.Elem()
	for e.Kind() == reflect.Ptr {
		e = e.Elem()
	}

	empty := reflect.MakeSlice(t, 0, 0)

	v.Set(reflect.Indirect(empty))

	for rows.Next() {
		instance, err := self.Scanner.Scan(rows, e)
		if err != nil {
			return err
		}

		val := reflect.ValueOf(instance)

		if t.Elem().Kind() != reflect.Ptr {
			val = reflect.Indirect(val)
		}

		v.Set(reflect.Append(v, val))
	}

	if err := rows.Err(); err != nil {
		return err
	}

	return nil
}

func (self *executor) batch(tx *sql.Tx, batch ex.Batch, data interface{}) error {

	for _, c := range batch {
		if err := self.executeTx(tx, c, data); err != nil {
			return err
		}
	}

	return nil
}

func (self *executor) stmt(tx *sql.Tx, stmt ex.Statement) error {

	_, err := tx.Exec(stmt.Stmt, stmt.Args...)
	return err
}
