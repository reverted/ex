package sql

import (
	"fmt"
	"strings"

	"github.com/reverted/ex"
)

func NewMysqlFormatter() *mysqlFormatter {
	return &mysqlFormatter{}
}

type mysqlFormatter struct{}

func (self *mysqlFormatter) FormatQuery(req ex.Command) ex.Statement {

	var stmt string
	var args []interface{}

	stmt = "SELECT * FROM " + req.Resource

	if clause, whereArgs := self.FormatWhere(req.Where); clause != "" {
		stmt += " WHERE " + clause
		args = append(args, whereArgs...)
	}

	if clause := self.FormatOrder(req.Order); clause != "" {
		stmt += " ORDER BY " + clause
	}

	if clause := self.FormatLimit(req.Limit); clause != "" {
		stmt += " LIMIT " + clause
	}

	if clause := self.FormatOffset(req.Offset); clause != "" {
		stmt += " OFFSET " + clause
	}

	return ex.Exec(stmt, args...)
}

func (self *mysqlFormatter) FormatDelete(req ex.Command) ex.Statement {

	var stmt string
	var args []interface{}

	stmt = "DELETE FROM " + req.Resource

	if clause, whereArgs := self.FormatWhere(req.Where); clause != "" {
		stmt += " WHERE " + clause
		args = append(args, whereArgs...)
	}

	if clause := self.FormatLimit(req.Limit); clause != "" {
		stmt += " LIMIT " + clause
	}

	return ex.Exec(stmt, args...)
}

func (self *mysqlFormatter) FormatInsert(req ex.Command) ex.Statement {

	columnNames, args := self.FormatValues(req.Values)

	stmt := "INSERT INTO " + req.Resource + " SET " + columnNames

	if clause := self.FormatConflict(req.OnConflict); clause != "" {
		stmt += " ON " + clause
	}

	return ex.Exec(stmt, args...)
}

func (self *mysqlFormatter) FormatUpdate(req ex.Command) ex.Statement {

	var stmt string
	var args []interface{}

	columnNames, args := self.FormatValues(req.Values)

	stmt = "UPDATE " + req.Resource + " SET " + columnNames

	if clause, whereArgs := self.FormatWhere(req.Where); clause != "" {
		stmt += " WHERE " + clause
		args = append(args, whereArgs...)
	}

	if clause := self.FormatLimit(req.Limit); clause != "" {
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
			columns = append(columns, fmt.Sprintf("%s=?", k))
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

	switch c := conflict.(type) {

	case ex.OnConflictUpdate:
		return self.FormatConflictUpdate(c)

	default:
		return ""
	}
}

func (self *mysqlFormatter) FormatConflictUpdate(conflict ex.OnConflictUpdate) string {
	var columns []string

	for _, c := range conflict {
		columns = append(columns, fmt.Sprintf("%s=VALUES(%s)", c, c))
	}

	if len(columns) > 0 {
		return "DUPLICATE KEY UPDATE " + strings.Join(columns, ",")
	} else {
		return ""
	}
}

func (self *mysqlFormatter) FormatWhere(where ex.Where) (string, []interface{}) {

	var columns []string
	var args []interface{}

	for k, v := range where {
		switch value := v.(type) {

		case ex.Literal:
			columns = append(columns, fmt.Sprintf("%s = %s", k, value.Arg))
		case ex.Eq:
			columns = append(columns, fmt.Sprintf("%s = ?", k))
			args = append(args, value.Arg)
		case ex.NotEq:
			columns = append(columns, fmt.Sprintf("%s != ?", k))
			args = append(args, value.Arg)
		case ex.Gt:
			columns = append(columns, fmt.Sprintf("%s > ?", k))
			args = append(args, value.Arg)
		case ex.GtEq:
			columns = append(columns, fmt.Sprintf("%s >= ?", k))
			args = append(args, value.Arg)
		case ex.Lt:
			columns = append(columns, fmt.Sprintf("%s < ?", k))
			args = append(args, value.Arg)
		case ex.LtEq:
			columns = append(columns, fmt.Sprintf("%s <= ?", k))
			args = append(args, value.Arg)
		case ex.Like:
			columns = append(columns, fmt.Sprintf("%s LIKE ?", k))
			args = append(args, "%"+value.Arg+"%")
		case ex.NotLike:
			columns = append(columns, fmt.Sprintf("%s NOT LIKE ?", k))
			args = append(args, "%"+value.Arg+"%")
		case ex.Is:
			columns = append(columns, fmt.Sprintf("%s IS %v", k, value.Arg))
		case ex.IsNot:
			columns = append(columns, fmt.Sprintf("%s IS NOT %v", k, value.Arg))
		case ex.In:
			columns = append(columns, fmt.Sprintf("%s IN (%s)", k, self.formatIn(value)))
			args = append(args, value...)
		case ex.NotIn:
			columns = append(columns, fmt.Sprintf("%s NOT IN (%s)", k, self.formatIn(value)))
			args = append(args, value...)
		case ex.Btwn:
			columns = append(columns, fmt.Sprintf("%s BETWEEN ? AND ?", k))
			args = append(args, value.Start, value.End)
		case ex.NotBtwn:
			columns = append(columns, fmt.Sprintf("%s NOT BETWEEN ? AND ?", k))
			args = append(args, value.Start, value.End)
		default:
			columns = append(columns, fmt.Sprintf("%s = ?", k))
			args = append(args, value)
		}
	}

	return strings.Join(columns, " AND "), args
}

func (self *mysqlFormatter) formatIn(args []interface{}) string {
	qs := strings.Repeat("?", len(args))
	return strings.Join(strings.Split(qs, ""), ",")
}
