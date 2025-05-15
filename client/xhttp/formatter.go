package xhttp

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strings"

	"github.com/reverted/ex"
)

func NewFormatter(url *url.URL) *formatter {
	return &formatter{url}
}

type formatter struct {
	*url.URL
}

func (f *formatter) Format(req ex.Request) (*http.Request, error) {
	switch c := req.(type) {
	case ex.Statement:
		return f.FormatStatement(c)

	case ex.Command:
		return f.FormatCommand(c)

	case ex.Batch:
		return f.FormatBatch(c)

	default:
		return nil, errors.New("unsupported req")
	}
}

func (f *formatter) FormatStatement(stmt ex.Statement) (*http.Request, error) {

	body, err := f.FormatBody(stmt)
	if err != nil {
		return nil, err
	}

	url := *f.URL
	url.Path = path.Join(url.Path, ":exec")

	return http.NewRequest("POST", url.String(), body)
}

func (f *formatter) FormatBatch(batch ex.Batch) (*http.Request, error) {

	body, err := f.FormatBody(batch)
	if err != nil {
		return nil, err
	}

	url := *f.URL
	url.Path = path.Join(url.Path, ":batch")

	return http.NewRequest("POST", url.String(), body)
}

func (f *formatter) FormatCommand(cmd ex.Command) (*http.Request, error) {

	switch strings.ToUpper(cmd.Action) {
	case "QUERY", "DELETE", "INSERT", "UPDATE":
		return f.FormatRequest(cmd)

	default:
		return nil, errors.New("unsupported cmd")
	}
}

func (f *formatter) FormatRequest(cmd ex.Command) (*http.Request, error) {

	method := methods[cmd.Action]

	params, err := f.FormatParams(cmd)
	if err != nil {
		return nil, err
	}

	body, err := f.FormatBodyForMethod(method, cmd.Values)
	if err != nil {
		return nil, err
	}

	url := *f.URL
	url.Path = path.Join(url.Path, cmd.Resource)
	url.RawQuery = params.Encode()

	r, err := http.NewRequest(method, url.String(), body)
	if err != nil {
		return nil, err
	}

	headers, err := f.FormatHeaders(cmd)
	if err != nil {
		return nil, err
	}

	for name, value := range headers {
		r.Header.Add(name, value)
	}

	return r, nil
}

func (f *formatter) FormatParams(cmd ex.Command) (url.Values, error) {

	params := url.Values{}

	for k, v := range cmd.Where {
		key, value, err := ex.FormatWhereArg(k, v)
		if err != nil {
			return nil, err
		}
		params.Add(key, fmt.Sprintf("%v", value))
	}

	return params, nil
}

func (f *formatter) FormatHeaders(cmd ex.Command) (map[string]string, error) {
	res := map[string]string{}

	if len(cmd.ColumnConfig) > 0 {
		res["X-Columns"] = strings.Join(cmd.ColumnConfig, ",")
	}

	if len(cmd.GroupConfig) > 0 {
		res["X-Group-By"] = strings.Join(cmd.GroupConfig, ",")
	}

	if len(cmd.OrderConfig) > 0 {
		res["X-Order-By"] = strings.Join(cmd.OrderConfig, ",")
	}

	if cmd.LimitConfig > 0 {
		res["X-Limit"] = fmt.Sprintf("%v", cmd.LimitConfig)
	}

	if cmd.OffsetConfig > 0 {
		res["X-Offset"] = fmt.Sprintf("%v", cmd.OffsetConfig)
	}

	if c := cmd.OnConflictConfig.Constraint; len(c) > 0 {
		res["X-On-Conflict-Constraint"] = strings.Join(c, ",")
	}

	if c := cmd.OnConflictConfig.Update; len(c) > 0 {
		res["X-On-Conflict-Update"] = strings.Join(c, ",")
	}

	if c := cmd.OnConflictConfig.Ignore; c != "" {
		res["X-On-Conflict-Ignore"] = c
	}

	if c := cmd.OnConflictConfig.Error; c != "" {
		res["X-On-Conflict-Error"] = c
	}

	return res, nil
}

func (f *formatter) FormatBodyForMethod(method string, values ex.Values) (io.Reader, error) {

	switch method {
	case "PUT", "POST":
		return f.FormatBody(values)
	default:
		return http.NoBody, nil
	}
}

func (f *formatter) FormatBody(values any) (io.Reader, error) {

	content, err := json.Marshal(values)
	if err != nil {
		return nil, err
	}

	return bytes.NewBuffer(content), nil
}

var methods = map[string]string{
	"QUERY":  "GET",
	"DELETE": "DELETE",
	"INSERT": "POST",
	"UPDATE": "PUT",
}
