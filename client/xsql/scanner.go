package xsql

import (
	"database/sql"
	"errors"
	"reflect"
	"strconv"
	"time"

	"github.com/go-sql-driver/mysql"
)

type Scannable interface {
	Scan(Rows) error
}

func NewScanner() *scanner {
	return &scanner{}
}

type scanner struct{}

func (self *scanner) Scan(rows Rows, data interface{}) error {

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

	if t.Kind() == reflect.Slice {
		return self.queryRows(rows, t, v)
	} else {
		return self.queryRow(rows, t, v)
	}
}

func (self *scanner) queryRows(rows Rows, t reflect.Type, v reflect.Value) error {

	e := t.Elem()
	for e.Kind() == reflect.Ptr {
		e = e.Elem()
	}

	empty := reflect.MakeSlice(t, 0, 0)

	v.Set(reflect.Indirect(empty))

	for rows.Next() {
		val, err := self.scanInstance(rows, e)
		if err != nil {
			return err
		}

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

func (self *scanner) queryRow(rows Rows, t reflect.Type, v reflect.Value) error {

	if rows.Next() {
		val, err := self.scanInstance(rows, t)
		if err != nil {
			return err
		}

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

func (self *scanner) scanInstance(rows Rows, t reflect.Type) (reflect.Value, error) {
	var instance interface{}

	switch t.Kind() {
	case reflect.Map:
		instance = reflect.MakeMap(t).Interface()
	default:
		instance = reflect.New(t).Interface()
	}

	if err := self.scanRow(rows, instance); err != nil {
		return reflect.Value{}, err
	}

	return reflect.ValueOf(instance), nil
}

func (self *scanner) scanRow(rows Rows, instance interface{}) error {
	switch item := instance.(type) {
	case Scannable:
		return item.Scan(rows)

	case map[string]interface{}:
		return self.scanMap(rows, item)

	default:
		return self.scanTags(rows, item)
	}
}

func (self *scanner) scanMap(rows Rows, item map[string]interface{}) error {

	types, values, err := self.scanValues(rows)
	if err != nil {
		return err
	}

	for i, t := range types {
		v := reflect.ValueOf(values[i])

		for v.Kind() == reflect.Ptr {
			v = v.Elem()
		}

		item[t.Name()] = self.scanValue(v.Interface(), t.DatabaseTypeName())
	}

	return nil
}

func (self *scanner) scanValues(rows Rows) ([]ColumnType, []interface{}, error) {

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

func (self *scanner) scanTags(rows Rows, item interface{}) error {

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

	fields := self.fields(t, v)
	values := make([]interface{}, len(types))

	for i, typ := range types {
		instance, ok := fields[typ.Name()]
		if !ok {
			return errors.New("Tag not found")
		}

		values[i] = instance
	}

	if err := rows.Scan(values...); err != nil {
		return err
	}

	return nil
}

func (self *scanner) fields(t reflect.Type, v reflect.Value) map[string]interface{} {

	fields := map[string]interface{}{}

	for i := 0; i < t.NumField(); i++ {
		tf := t.Field(i)
		vf := v.Field(i)

		if vf.Kind() != reflect.Ptr {
			vf = vf.Addr()
		}

		fields[tf.Tag.Get("json")] = vf.Interface()
	}

	return fields
}
