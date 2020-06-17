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
	case "QUERY":
		return self.FormatQuery(cmd)

	case "DELETE":
		return self.FormatDelete(cmd)

	case "INSERT":
		return self.FormatInsert(cmd)

	case "UPDATE":
		return self.FormatUpdate(cmd)

	default:
		return nil, errors.New("Unsupported cmd")
	}
}

func (self *formatter) FormatQuery(cmd ex.Command) (*http.Request, error) {

	params, err := self.FormatParams(cmd)
	if err != nil {
		return nil, err
	}

	url := *self.URL
	url.Path = path.Join(url.Path, cmd.Resource)
	url.RawQuery = params.Encode()

	return self.FormatHttpRequest("GET", url.String(), nil)
}

func (self *formatter) FormatDelete(req ex.Command) (*http.Request, error) {

	params, err := self.FormatParams(req)
	if err != nil {
		return nil, err
	}

	url := *self.URL
	url.Path = path.Join(url.Path, req.Resource)
	url.RawQuery = params.Encode()

	return self.FormatHttpRequest("DELETE", url.String(), nil)
}

func (self *formatter) FormatInsert(cmd ex.Command) (*http.Request, error) {

	params, err := self.FormatParams(cmd)
	if err != nil {
		return nil, err
	}

	body, err := self.FormatBody(cmd.Values)
	if err != nil {
		return nil, err
	}

	url := *self.URL
	url.Path = path.Join(url.Path, cmd.Resource)
	url.RawQuery = params.Encode()

	r, err := self.FormatHttpRequest("POST", url.String(), bytes.NewBuffer(body))
	if err != nil {
		return nil, err
	}

	switch c := cmd.OnConflict.(type) {
	case ex.OnConflictUpdate:
		if len(c) > 0 {
			r.Header.Add("X-On-Conflict", strings.Join(c, ","))
		}
	}

	return r, nil
}

func (self *formatter) FormatUpdate(cmd ex.Command) (*http.Request, error) {

	params, err := self.FormatParams(cmd)
	if err != nil {
		return nil, err
	}

	body, err := self.FormatBody(cmd.Values)
	if err != nil {
		return nil, err
	}

	url := *self.URL
	url.Path = path.Join(url.Path, cmd.Resource)
	url.RawQuery = params.Encode()

	return self.FormatHttpRequest("PUT", url.String(), bytes.NewBuffer(body))
}

func (self *formatter) FormatBatch(batch ex.Batch) (*http.Request, error) {

	body, err := self.FormatBody(batch)
	if err != nil {
		return nil, err
	}

	url := *self.URL
	url.Path = path.Join(url.Path, ":batch")

	return self.FormatHttpRequest("POST", url.String(), bytes.NewBuffer(body))
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

	if len(cmd.Order) > 0 {
		params.Add(":order", strings.Join(cmd.Order, ","))
	}

	if cmd.Limit.Arg > 0 {
		params.Add(":limit", fmt.Sprintf("%v", cmd.Limit.Arg))
	}

	if cmd.Offset.Arg > 0 {
		params.Add(":offset", fmt.Sprintf("%v", cmd.Offset.Arg))
	}

	return params, nil
}

func (self *formatter) FormatBody(values interface{}) ([]byte, error) {

	return json.Marshal(values)
}

func (self *formatter) FormatHttpRequest(method, url string, body io.Reader) (*http.Request, error) {

	return http.NewRequest(method, url, body)
}
