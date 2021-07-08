package xsql

import (
	"errors"
	"fmt"
	"strings"

	"github.com/reverted/ex"
)

func NewMysqlFormatter() *mysqlFormatter {
	return &mysqlFormatter{}
}

type mysqlFormatter struct{}

func (self *mysqlFormatter) Format(cmd ex.Command) (ex.Statement, error) {
	switch strings.ToUpper(cmd.Action) {
	case "QUERY":
		return self.FormatQuery(cmd), nil

	case "DELETE":
		return self.FormatDelete(cmd), nil

	case "INSERT":
		return self.FormatInsert(cmd), nil

	case "UPDATE":
		return self.FormatUpdate(cmd), nil

	default:
		return ex.Statement{}, errors.New("Unsupported cmd")
	}
}

func (self *mysqlFormatter) FormatQuery(cmd ex.Command) ex.Statement {

	var stmt string
	var args []interface{}

	stmt = "SELECT * FROM " + cmd.Resource

	if clause, whereArgs := self.FormatWhere(cmd.Where); clause != "" {
		stmt += " WHERE " + clause
		args = append(args, whereArgs...)
	}

	if clause := self.FormatOrder(cmd.Order); clause != "" {
		stmt += " ORDER BY " + clause
	}

	if clause := self.FormatLimit(cmd.Limit); clause != "" {
		stmt += " LIMIT " + clause
	}

	if clause := self.FormatOffset(cmd.Offset); clause != "" {
		stmt += " OFFSET " + clause
	}

	return ex.Exec(stmt, args...)
}

func (self *mysqlFormatter) FormatDelete(cmd ex.Command) ex.Statement {

	var stmt string
	var args []interface{}

	stmt = "DELETE FROM " + cmd.Resource

	if clause, whereArgs := self.FormatWhere(cmd.Where); clause != "" {
		stmt += " WHERE " + clause
		args = append(args, whereArgs...)
	}

	if clause := self.FormatOrder(cmd.Order); clause != "" {
		stmt += " ORDER BY " + clause
	}

	if clause := self.FormatLimit(cmd.Limit); clause != "" {
		stmt += " LIMIT " + clause
	}

	return ex.Exec(stmt, args...)
}

func (self *mysqlFormatter) FormatInsert(cmd ex.Command) ex.Statement {

	var stmt string
	var args []interface{}

	stmt = "INSERT INTO " + cmd.Resource

	if columns, columnArgs := self.FormatValues(cmd.Values); columns != "" {
		stmt += " SET " + columns
		args = append(args, columnArgs...)
	}

	if clause := self.FormatConflict(cmd.OnConflict); clause != "" {
		stmt += " ON " + clause
	}

	return ex.Exec(stmt, args...)
}

func (self *mysqlFormatter) FormatUpdate(cmd ex.Command) ex.Statement {

	var stmt string
	var args []interface{}

	stmt = "UPDATE " + cmd.Resource

	if columns, columnArgs := self.FormatValues(cmd.Values); columns != "" {
		stmt += " SET " + columns
		args = append(args, columnArgs...)
	}

	if clause, whereArgs := self.FormatWhere(cmd.Where); clause != "" {
		stmt += " WHERE " + clause
		args = append(args, whereArgs...)
	}

	if clause := self.FormatOrder(cmd.Order); clause != "" {
		stmt += " ORDER BY " + clause
	}

	if clause := self.FormatLimit(cmd.Limit); clause != "" {
		stmt += " LIMIT " + clause
	}

	return ex.Exec(stmt, args...)
}

func (self *mysqlFormatter) FormatValues(values ex.Values) (string, []interface{}) {

	var columns []string
	var args []interface{}

	for k, v := range values {
		switch value := v.(type) {
		case ex.Literal:
			columns = append(columns, fmt.Sprintf("%s=%s", k, value.Arg))

		default:
			columns = append(columns, fmt.Sprintf("%s = ?", k))
			args = append(args, v)
		}
	}

	return strings.Join(columns, ","), args
}

func (self *mysqlFormatter) FormatOrder(order ex.Order) string {

	return strings.Join(order, ",")
}

func (self *mysqlFormatter) FormatLimit(limit ex.Limit) string {
	if limit.Arg > 0 {
		return fmt.Sprintf("%v", limit.Arg)
	} else {
		return ""
	}
}

func (self *mysqlFormatter) FormatOffset(offset ex.Offset) string {
	if offset.Arg > 0 {
		return fmt.Sprintf("%v", offset.Arg)
	} else {
		return ""
	}
}

func (self *mysqlFormatter) FormatConflict(conflict ex.OnConflict) string {

	if c := conflict.Update; len(c) > 0 {
		return self.FormatConflictUpdate(c)
	}

	if c := conflict.Ignore; c != "" {
		return self.FormatConflictIgnore(c)
	}

	if c := conflict.Error; c != "" {
		return self.FormatConflictError(c)
	}

	return ""
}

func (self *mysqlFormatter) FormatConflictUpdate(conflict ex.OnConflictUpdate) string {
	var columns []string

	for _, c := range conflict {
		columns = append(columns, fmt.Sprintf("%s = VALUES(%s)", c, c))
	}

	if len(columns) > 0 {
		return "DUPLICATE KEY UPDATE " + strings.Join(columns, ",")
	} else {
		return ""
	}
}

func (self *mysqlFormatter) FormatConflictIgnore(conflict ex.OnConflictIgnore) string {

	if conflict == "true" {
		return fmt.Sprintf("DUPLICATE KEY UPDATE id = id")
	} else {
		return fmt.Sprintf("DUPLICATE KEY UPDATE %s = %s", conflict, conflict)
	}
}

func (self *mysqlFormatter) FormatConflictError(conflict ex.OnConflictError) string {

	return ""
}

func (self *mysqlFormatter) FormatWhereArg(k string, v interface{}) (string, []interface{}) {
	switch value := v.(type) {

	case ex.Literal:
		return fmt.Sprintf("%s = %s", k, value.Arg), nil
	case ex.Eq:
		return fmt.Sprintf("%s = ?", k), []interface{}{value.Arg}
	case ex.NotEq:
		return fmt.Sprintf("%s != ?", k), []interface{}{value.Arg}
	case ex.Gt:
		return fmt.Sprintf("%s > ?", k), []interface{}{value.Arg}
	case ex.GtEq:
		return fmt.Sprintf("%s >= ?", k), []interface{}{value.Arg}
	case ex.Lt:
		return fmt.Sprintf("%s < ?", k), []interface{}{value.Arg}
	case ex.LtEq:
		return fmt.Sprintf("%s <= ?", k), []interface{}{value.Arg}
	case ex.Like:
		return fmt.Sprintf("%s LIKE ?", k), []interface{}{"%" + value.Arg + "%"}
	case ex.NotLike:
		return fmt.Sprintf("%s NOT LIKE ?", k), []interface{}{"%" + value.Arg + "%"}
	case ex.Is:
		return fmt.Sprintf("%s IS %v", k, self.formatIs(value.Arg)), nil
	case ex.IsNot:
		return fmt.Sprintf("%s IS NOT %v", k, self.formatIs(value.Arg)), nil
	case ex.In:
		return fmt.Sprintf("%s IN (%s)", k, self.formatIn(value)), value
	case ex.NotIn:
		return fmt.Sprintf("%s NOT IN (%s)", k, self.formatIn(value)), value
	case ex.Btwn:
		return fmt.Sprintf("%s BETWEEN ? AND ?", k), []interface{}{value.Start, value.End}
	case ex.NotBtwn:
		return fmt.Sprintf("%s NOT BETWEEN ? AND ?", k), []interface{}{value.Start, value.End}
	default:
		return fmt.Sprintf("%s = ?", k), []interface{}{value}
	}
}

func (self *mysqlFormatter) FormatWhere(where ex.Where) (string, []interface{}) {

	var columns []string
	var args []interface{}

	for k, v := range where {
		column, arg := self.FormatWhereArg(k, v)
		if column != "" {
			columns = append(columns, column)
		}

		if arg != nil {
			args = append(args, arg...)
		}
	}

	return strings.Join(columns, " AND "), args
}

func (self *mysqlFormatter) formatIn(args []interface{}) string {
	qs := strings.Repeat("?", len(args))
	return strings.Join(strings.Split(qs, ""), ",")
}

func (self *mysqlFormatter) formatIs(arg interface{}) string {
	return "NULL"
}
