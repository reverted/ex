package xhttp

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
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
	return func(e *executor) {
		WithFormatter(NewFormatter(target))(e)
	}
}

func WithFormatter(formatter Formatter) opt {
	return func(e *executor) {
		e.Formatter = formatter
	}
}

func WithTracer(tracer Tracer) opt {
	return func(e *executor) {
		e.Tracer = tracer
	}
}

func WithClient(client Client) opt {
	return func(e *executor) {
		e.Client = client
	}
}

func NewExecutor(logger Logger, opts ...opt) *executor {

	url, _ := url.Parse("http://localhost:8080")

	executor := &executor{
		Logger:    logger,
		Formatter: NewFormatter(url),
		Tracer:    noopTracer{},
		Client:    http.DefaultClient,
	}

	for _, opt := range opts {
		opt(executor)
	}

	return executor
}

type executor struct {
	Logger
	Formatter
	Tracer
	Client
}

func (e *executor) Execute(ctx context.Context, req ex.Request, data interface{}) (bool, error) {

	r, err := e.Formatter.Format(req)
	if err != nil {
		return false, err
	}

	return e.exec(ctx, r, data)
}

func (e *executor) exec(ctx context.Context, r *http.Request, data interface{}) (bool, error) {

	e.Logger.Infof(">>> %v", r.URL)

	e.Tracer.InjectSpan(ctx, r)

	resp, err := e.Client.Do(r.WithContext(ctx))
	if err != nil {
		return true, err
	}

	defer resp.Body.Close()

	switch {
	case resp.StatusCode >= 500:
		bodyBytes, _ := io.ReadAll(resp.Body)
		return true, fmt.Errorf("server error: [%v] %s", resp.StatusCode, string(bodyBytes))

	case resp.StatusCode >= 400:
		bodyBytes, _ := io.ReadAll(resp.Body)
		return false, fmt.Errorf("client error: [%v] %s", resp.StatusCode, string(bodyBytes))

	case data != nil:
		return false, json.NewDecoder(resp.Body).Decode(data)

	default:
		return false, nil
	}
}

type noopSpan struct{}

func (s noopSpan) Finish() {}

type noopTracer struct{}

func (t noopTracer) StartSpan(ctx context.Context, name string, tags ...ex.SpanTag) (ex.Span, context.Context) {
	return noopSpan{}, ctx
}

func (t noopTracer) InjectSpan(ctx context.Context, r *http.Request) {
}

func (t noopTracer) ExtractSpan(r *http.Request, name string) (ex.Span, context.Context) {
	return noopSpan{}, r.Context()
}
