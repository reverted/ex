package client

import (
	"context"
	"net/http"
	"time"

	"github.com/reverted/ex"
)

type Logger interface {
	Errorf(string, ...interface{})
	Infof(string, ...interface{})
}

type Executor interface {
	Execute(context.Context, ex.Request, interface{}) (bool, error)
}

type Tracer interface {
	StartSpan(context.Context, string, ...ex.SpanTag) (ex.Span, context.Context)
}

type Client interface {
	Exec(ex.Request, ...interface{}) error
	ExecContext(context.Context, ex.Request, ...interface{}) error
}

func WithExecutor(executor Executor) opt {
	return func(c *client) {
		c.Executor = executor
	}
}

func WithTracer(tracer Tracer) opt {
	return func(c *client) {
		c.Tracer = tracer
	}
}

type opt func(*client)

func New(logger Logger, opts ...opt) *client {

	client := &client{
		Logger: logger,
		Tracer: noopTracer{},
	}

	for _, opt := range opts {
		opt(client)
	}

	return client
}

type client struct {
	Logger
	Executor
	Tracer
}

func (c *client) Exec(req ex.Request, res ...interface{}) error {
	return c.ExecContext(context.Background(), req, res...)
}

func (c *client) ExecContext(ctx context.Context, req ex.Request, res ...interface{}) error {

	span, spanCtx := c.Tracer.StartSpan(ctx, "exec")
	defer span.Finish()

	if len(res) > 0 {
		return c.execute(spanCtx, req, res[0])
	} else {
		return c.execute(spanCtx, req, nil)
	}
}

func (c *client) execute(ctx context.Context, req ex.Request, data interface{}) error {

	var err error
	var retry bool

	attempts := []int{0, 1, 2, 5, 10, 30, 60, 120, 300, 600}

	for i, interval := range attempts {
		time.Sleep(time.Duration(interval) * time.Second)

		c.Logger.Infof(">>> %+v", req)

		span, spanCtx := c.Tracer.StartSpan(ctx, "exec", ex.SpanTag{Key: "attempt", Value: i})
		defer span.Finish()

		if retry, err = c.Executor.Execute(spanCtx, req, data); !retry {
			break
		}

		if next := i + 1; next < len(attempts) {
			c.Logger.Infof(">>> %+v [failed] retry in %ds", req, attempts[next])
		} else {
			c.Logger.Errorf(">>> %+v [failed] %v", req, err)
		}
	}

	return err
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
