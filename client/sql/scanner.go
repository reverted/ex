package sql

import (
	"database/sql"
	"errors"
	"reflect"
	"strconv"
	"time"

	"github.com/go-sql-driver/mysql"
)

type Scannable interface {
	Scan(rows *sql.Rows, cols ...string) error
}

func NewScanner() *scanner {
	return &scanner{}
}

type scanner struct{}

func (self *scanner) Scan(rows *sql.Rows, t reflect.Type) (interface{}, error) {

	var instance interface{}

	switch t.Kind() {
	case reflect.Map:
		instance = reflect.MakeMap(t).Interface()
	default:
		instance = reflect.New(t).Interface()
	}

	return instance, self.scan(rows, instance)
}

func (self *scanner) scan(rows *sql.Rows, instance interface{}) error {
	switch item := instance.(type) {
	case Scannable:
		return self.scanScannable(rows, item)

	case map[string]interface{}:
		return self.scanMap(rows, item)

	default:
		return self.scanTags(rows, item)
	}
}

func (self *scanner) scanScannable(rows *sql.Rows, item Scannable) error {

	cols, err := rows.Columns()
	if err != nil {
		return err
	}

	return item.Scan(rows, cols...)
}

func (self *scanner) scanMap(rows *sql.Rows, item map[string]interface{}) error {

	types, values, err := self.scanColumns(rows)
	if err != nil {
		return err
	}

	for i, t := range types {
		v := reflect.ValueOf(values[i])

		if v.Kind() == reflect.Ptr {
			v = v.Elem()
		}

		item[t.Name()] = self.scanValue(v.Interface(), t.DatabaseTypeName())
	}

	return nil
}

func (self *scanner) scanColumns(rows *sql.Rows) ([]*sql.ColumnType, []interface{}, error) {

	types, err := rows.ColumnTypes()
	if err != nil {
		return nil, nil, err
	}

	values := make([]interface{}, len(types))

	for i, t := range types {
		values[i] = reflect.New(t.ScanType()).Interface()
	}

	if err := rows.Scan(values...); err != nil {
		return nil, nil, err
	}

	return types, values, nil
}

func (self *scanner) scanValue(value interface{}, dbTypeName string) interface{} {

	switch v := value.(type) {
	case sql.NullString:
		return v.String

	case sql.NullInt64:
		return v.Int64

	case sql.NullFloat64:
		return v.Float64

	case sql.NullBool:
		return v.Bool

	case mysql.NullTime:
		return self.scanNullTime(v.Time, dbTypeName)

	case sql.RawBytes:
		return self.scanRawBytes(string(v), dbTypeName)

	case int32:
		return int(v)

	case int64:
		return int(v)

	default:
		return value
	}
}

func (self *scanner) scanNullTime(t time.Time, dbTypeName string) interface{} {
	switch dbTypeName {
	case "DATE":
		return t.Format("2006-01-02")
	default:
		return t.Format(time.RFC3339)
	}
}

func (self *scanner) scanRawBytes(value string, dbTypeName string) interface{} {
	switch dbTypeName {
	case "VARCHAR", "TEXT":
		return self.scanString(value)
	case "DECIMAL":
		val, _ := strconv.ParseFloat(value, 64)
		return val
	default:
		return value
	}
}

func (self *scanner) scanString(value string) interface{} {
	switch value {
	case "true":
		return true
	case "false":
		return false
	default:
		return value
	}
}

func (self *scanner) scanTags(rows *sql.Rows, item interface{}) error {

	t := reflect.TypeOf(item)
	v := reflect.ValueOf(item)

	for t.Kind() == reflect.Ptr {
		t = t.Elem()
		v = v.Elem()
	}

	types, err := rows.ColumnTypes()
	if err != nil {
		return err
	}

	if len(types) != t.NumField() {
		return errors.New("Invalid field length")
	}

	values := make([]interface{}, len(types))

	for i, typ := range types {
		if values[i], err = self.scanField(t, v, typ); err != nil {
			return err
		}
	}

	if err := rows.Scan(values...); err != nil {
		return err
	}

	return nil
}

func (self *scanner) scanField(t reflect.Type, v reflect.Value, typ *sql.ColumnType) (interface{}, error) {

	for i := 0; i < t.NumField(); i++ {
		tf := t.Field(i)
		vf := v.Field(i)

		if vf.Kind() != reflect.Ptr {
			vf = vf.Addr()
		}

		if tf.Tag.Get("json") == typ.Name() {
			return vf.Interface(), nil
		}
	}

	return nil, errors.New("Tag not found")
}
