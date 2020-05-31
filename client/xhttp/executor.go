package xhttp

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"

	"github.com/reverted/ex"
)

type Logger interface {
	Infof(format string, a ...interface{})
}

type Tracer interface {
	InjectSpan(context.Context, *http.Request)
}

type Formatter interface {
	Format(ex.Request) (*http.Request, error)
}

type Client interface {
	Do(*http.Request) (*http.Response, error)
}

type opt func(*executor)

func WithTarget(target *url.URL) opt {
	return func(self *executor) {
		WithFormatter(NewFormatter(target))(self)
	}
}

func WithFormatter(formatter Formatter) opt {
	return func(self *executor) {
		self.Formatter = formatter
	}
}

func WithTracer(tracer Tracer) opt {
	return func(self *executor) {
		self.Tracer = tracer
	}
}

func WithClient(client Client) opt {
	return func(self *executor) {
		self.Client = client
	}
}

func NewExecutor(logger Logger, opts ...opt) *executor {

	executor := &executor{Logger: logger}

	for _, opt := range opts {
		opt(executor)
	}

	if executor.Formatter == nil {
		url, _ := url.Parse("http://localhost:8080")
		WithTarget(url)(executor)
	}

	if executor.Client == nil {
		WithClient(http.DefaultClient)(executor)
	}

	return executor
}

type executor struct {
	Logger
	Formatter
	Tracer
	Client
}

func (self *executor) Execute(ctx context.Context, req ex.Request, data interface{}) (bool, error) {

	r, err := self.Formatter.Format(req)
	if err != nil {
		return false, err
	}

	return self.exec(ctx, r, data)
}

func (self *executor) exec(ctx context.Context, r *http.Request, data interface{}) (bool, error) {

	self.Logger.Infof(">>> %v", r.URL)

	self.Tracer.InjectSpan(ctx, r)

	resp, err := self.Client.Do(r.WithContext(ctx))
	if err != nil {
		return true, err
	}

	defer resp.Body.Close()

	switch {
	case resp.StatusCode >= 500:
		return true, fmt.Errorf("server error: %v", resp.StatusCode)

	case resp.StatusCode >= 400:
		return false, fmt.Errorf("client error: %v", resp.StatusCode)

	case data != nil:
		return false, json.NewDecoder(resp.Body).Decode(data)

	default:
		return false, nil
	}
}
