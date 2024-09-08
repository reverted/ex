package xhttp

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
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

func (self *formatter) Format(req ex.Request) (*http.Request, error) {
	switch c := req.(type) {
	case ex.Command:
		return self.FormatCommand(c)

	case ex.Batch:
		return self.FormatBatch(c)

	default:
		return nil, errors.New("Unsupported req")
	}
}

func (self *formatter) FormatCommand(cmd ex.Command) (*http.Request, error) {

	switch strings.ToUpper(cmd.Action) {
	case "QUERY", "DELETE", "INSERT", "UPDATE":
		return self.FormatRequest(cmd)

	default:
		return nil, errors.New("Unsupported cmd")
	}
}

func (self *formatter) FormatRequest(cmd ex.Command) (*http.Request, error) {

	method := methods[cmd.Action]

	params, err := self.FormatParams(cmd)
	if err != nil {
		return nil, err
	}

	body, err := self.FormatBodyForMethod(method, cmd.Values)
	if err != nil {
		return nil, err
	}

	url := *self.URL
	url.Path = path.Join(url.Path, cmd.Resource)
	url.RawQuery = params.Encode()

	r, err := http.NewRequest(method, url.String(), body)
	if err != nil {
		return nil, err
	}

	headers, err := self.FormatHeaders(cmd)
	if err != nil {
		return nil, err
	}

	for name, value := range headers {
		r.Header.Add(name, value)
	}

	return r, nil
}

func (self *formatter) FormatBatch(batch ex.Batch) (*http.Request, error) {

	body, err := self.FormatBody(batch)
	if err != nil {
		return nil, err
	}

	url := *self.URL
	url.Path = path.Join(url.Path, ":batch")

	return http.NewRequest("POST", url.String(), body)
}

func (self *formatter) FormatParams(cmd ex.Command) (url.Values, error) {

	params := url.Values{}

	for k, v := range cmd.Where {
		key, value, err := ex.Format(k, v)
		if err != nil {
			return nil, err
		}
		params.Add(key, fmt.Sprintf("%v", value))
	}

	return params, nil
}

func (self *formatter) FormatHeaders(cmd ex.Command) (map[string]string, error) {
	res := map[string]string{}

	if len(cmd.Order) > 0 {
		res["X-Order-By"] = strings.Join(cmd.Order, ",")
	}

	if cmd.Limit.Arg > 0 {
		res["X-Limit"] = fmt.Sprintf("%v", cmd.Limit.Arg)
	}

	if cmd.Offset.Arg > 0 {
		res["X-Offset"] = fmt.Sprintf("%v", cmd.Offset.Arg)
	}

	if c := cmd.OnConflict.Update; len(c) > 0 {
		res["X-On-Conflict-Update"] = strings.Join(c, ",")
	}

	if c := cmd.OnConflict.Ignore; c != "" {
		res["X-On-Conflict-Ignore"] = fmt.Sprintf("%v", c)
	}

	if c := cmd.OnConflict.Error; c != "" {
		res["X-On-Conflict-Error"] = fmt.Sprintf("%v", c)
	}

	return res, nil
}

func (self *formatter) FormatBodyForMethod(method string, values interface{}) (*bytes.Buffer, error) {

	switch method {
	case "PUT", "POST":
		return self.FormatBody(values)
	default:
		return nil, nil
	}
}

func (self *formatter) FormatBody(values interface{}) (*bytes.Buffer, error) {

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
