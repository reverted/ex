package xsql

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"time"
)

type Scannable interface {
	Scan(Rows) error
}

func NewScanner() *scanner {
	return &scanner{}
}

type scanner struct{}

func (s *scanner) Scan(rows Rows, data interface{}) error {

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
		return s.queryRows(rows, t, v)
	} else {
		return s.queryRow(rows, t, v)
	}
}

func (s *scanner) queryRows(rows Rows, t reflect.Type, v reflect.Value) error {

	e := t.Elem()
	for e.Kind() == reflect.Ptr {
		e = e.Elem()
	}

	empty := reflect.MakeSlice(t, 0, 0)

	v.Set(reflect.Indirect(empty))

	for rows.Next() {
		val, err := s.scanInstance(rows, e)
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

func (s *scanner) queryRow(rows Rows, t reflect.Type, v reflect.Value) error {

	if rows.Next() {
		val, err := s.scanInstance(rows, t)
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
		return errors.New("not found")
	}

	return nil
}

func (s *scanner) scanInstance(rows Rows, t reflect.Type) (reflect.Value, error) {
	var instance interface{}

	switch t.Kind() {
	case reflect.Map:
		instance = reflect.MakeMap(t).Interface()
	default:
		instance = reflect.New(t).Interface()
	}

	if err := s.scanRow(rows, instance); err != nil {
		return reflect.Value{}, err
	}

	return reflect.ValueOf(instance), nil
}

func (s *scanner) scanRow(rows Rows, instance interface{}) error {
	switch item := instance.(type) {
	case Scannable:
		return item.Scan(rows)

	case map[string]interface{}:
		return s.scanMap(rows, item)

	default:
		return s.scanTags(rows, item)
	}
}

func (s *scanner) scanMap(rows Rows, item map[string]interface{}) error {

	types, values, err := s.scanValues(rows)
	if err != nil {
		return err
	}

	for i, t := range types {
		v := reflect.ValueOf(values[i])

		for v.Kind() == reflect.Ptr {
			v = v.Elem()
		}

		value, err := s.scanValue(v.Interface(), t.DatabaseTypeName())
		if err != nil {
			return err
		}

		item[t.Name()] = value
	}

	return nil
}

func (s *scanner) scanValues(rows Rows) ([]ColumnType, []interface{}, error) {

	types, err := rows.ColumnTypes()
	if err != nil {
		return nil, nil, err
	}

	values := make([]interface{}, len(types))

	for i, t := range types {
		switch t.DatabaseTypeName() {
		case "VARCHAR", "TEXT", "CHAR", "STRING", "BPCHAR":
			values[i] = &sql.NullString{}
		case "INT", "INT2", "INT4", "INT8", "INTEGER", "BIGINT", "SMALLINT", "SERIAL", "BIGSERIAL":
			values[i] = &sql.NullInt64{}
		case "FLOAT", "FLOAT4", "FLOAT8", "DOUBLE", "DECIMAL", "NUMERIC", "REAL":
			values[i] = &sql.NullFloat64{}
		case "BOOL", "BOOLEAN":
			values[i] = &sql.NullBool{}
		case "DATE", "DATETIME", "TIMESTAMP", "TIMESTAMPTZ":
			values[i] = &sql.NullTime{}
		default:
			values[i] = reflect.New(t.ScanType()).Interface()
		}
	}

	if err := rows.Scan(values...); err != nil {
		return nil, nil, err
	}

	return types, values, nil
}

func (s *scanner) scanValue(value interface{}, dbTypeName string) (interface{}, error) {

	switch v := value.(type) {
	case sql.NullString:
		return s.scanString(v.String, dbTypeName)

	case sql.NullInt32:
		return int(v.Int32), nil

	case sql.NullInt64:
		return int(v.Int64), nil

	case sql.NullFloat64:
		return v.Float64, nil

	case sql.NullBool:
		return v.Bool, nil

	case sql.NullTime:
		return s.scanNullTime(v.Time, dbTypeName), nil

	case sql.RawBytes:
		return s.scanRawBytes(string(v), dbTypeName)

	case int32:
		return int(v), nil

	case int64:
		return int(v), nil

	case string:
		return s.scanString(v, dbTypeName)

	case []byte:
		return s.scanString(string(v), dbTypeName)

	default:
		return value, nil
	}
}

func (s *scanner) scanNullTime(t time.Time, dbTypeName string) interface{} {
	switch dbTypeName {
	case "DATE":
		return t.Format("2006-01-02")
	default:
		return t.Format("2006-01-02T15:04:05.000Z07:00")
	}
}

func (s *scanner) scanRawBytes(value string, dbTypeName string) (interface{}, error) {
	switch dbTypeName {
	case "VARCHAR", "TEXT":
		return s.scanRawString(value), nil
	case "DECIMAL":
		return strconv.ParseFloat(value, 64)
	default:
		return value, nil
	}
}

func (s *scanner) scanRawString(value string) interface{} {
	switch value {
	case "true":
		return true
	case "false":
		return false
	default:
		return value
	}
}

func (s *scanner) scanString(value string, dbTypeName string) (interface{}, error) {
	switch dbTypeName {
	case "JSON":
		if value == "" {
			return nil, nil
		} else {
			var data interface{}
			return data, json.Unmarshal([]byte(value), &data)
		}
	default:
		return s.scanRawString(value), nil
	}
}

func (s *scanner) scanTags(rows Rows, item interface{}) error {

	scanned := map[string]interface{}{}
	if err := s.scanMap(rows, scanned); err != nil {
		return err
	}

	t := reflect.TypeOf(item)
	v := reflect.ValueOf(item)

	for t.Kind() == reflect.Ptr {
		t = t.Elem()
		v = v.Elem()
	}

	return s.assignFields(t, v, scanned)
}

func (s *scanner) assignFields(t reflect.Type, v reflect.Value, scanned map[string]interface{}) error {

	if t == reflect.TypeOf(json.RawMessage{}) {
		data, err := json.Marshal(scanned)
		if err != nil {
			return fmt.Errorf("failed to marshal map to json: %w", err)
		}
		if !v.CanSet() {
			return fmt.Errorf("cannot set value: %v", v)
		}
		v.SetBytes(data)
		return nil
	}

	if t.Kind() == reflect.Map {
		if !v.CanSet() {
			return fmt.Errorf("cannot set value: %v", v)
		}
		v.Set(reflect.ValueOf(scanned))
		return nil
	}

	if t.Kind() != reflect.Struct {
		return fmt.Errorf("expected a map or struct, got %v", t)
	}

	for i := 0; i < t.NumField(); i++ {
		tf := t.Field(i)
		vf := v.Field(i)

		for vf.Kind() == reflect.Ptr {
			vf = vf.Elem()
		}

		tag := tf.Tag.Get("json")

		item, ok := scanned[tag]
		if !ok {
			return fmt.Errorf("field not found: %s", tag)
		}

		if item == nil {
			continue
		}

		ts := reflect.TypeOf(item)
		vs := reflect.ValueOf(item)

		if !vf.CanSet() {
			return fmt.Errorf("cannot set field: %s", tag)
		}

		if ts.Kind() == reflect.Map {
			subMap, ok := item.(map[string]interface{})
			if ok {
				if err := s.assignFields(tf.Type, vf, subMap); err != nil {
					return err
				}
				continue
			}
		}

		if ts.Kind() == reflect.Slice {
			subSlice, ok := item.([]interface{})
			if ok {
				if tf.Type == reflect.TypeOf(json.RawMessage{}) {
					data, err := json.Marshal(subSlice)
					if err != nil {
						return fmt.Errorf("failed to marshal slice to json: %w", err)
					}
					vf.SetBytes(data)
					continue
				}

				sliceValue := reflect.MakeSlice(tf.Type, len(subSlice), len(subSlice))

				for j := 0; j < len(subSlice); j++ {
					elemValue := reflect.New(tf.Type.Elem()).Elem()
					elemType := tf.Type.Elem()

					if subMap, ok := subSlice[j].(map[string]interface{}); ok {
						if err := s.assignFields(elemType, elemValue, subMap); err != nil {
							return err
						}
						sliceValue.Index(j).Set(elemValue)

					} else if reflect.TypeOf(subSlice[j]).AssignableTo(elemType) {
						sliceValue.Index(j).Set(reflect.ValueOf(subSlice[j]))

					} else {
						return fmt.Errorf("cannot handle slice of %v", elemType)
					}
				}
				vf.Set(sliceValue)
				continue
			}
		}

		if !ts.AssignableTo(tf.Type) {
			return fmt.Errorf("field type mismatch: %s (%v, %v)", tag, ts, tf.Type)
		}

		vf.Set(vs)
	}

	return nil
}
