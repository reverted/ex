package client

import (
	"context"
	"net/http"
	"time"

	"github.com/reverted/ex"
)

type Logger interface {
	Errorf(string, ...any)
	Infof(string, ...any)
}

type Executor interface {
	Execute(context.Context, ex.Request, any) (bool, error)
}

type Tracer interface {
	StartSpan(context.Context, string, ...ex.SpanTag) (ex.Span, context.Context)
}

type Client interface {
	Exec(ex.Request, ...any) error
	ExecContext(context.Context, ex.Request, ...any) error
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

func WithBackoff(backoff ...int) opt {
	return func(c *client) {
		if len(backoff) > 0 {
			c.Backoff = backoff
		}
	}
}

type opt func(*client)

func New(logger Logger, opts ...opt) *client {

	client := &client{
		Logger:  logger,
		Tracer:  noopTracer{},
		Backoff: []int{0, 2, 5, 10, 30},
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

	Backoff []int
}

func (c *client) Exec(req ex.Request, res ...any) error {
	return c.ExecContext(context.Background(), req, res...)
}

func (c *client) ExecContext(ctx context.Context, req ex.Request, res ...any) error {

	span, spanCtx := c.Tracer.StartSpan(ctx, "exec")
	defer span.Finish()

	if len(res) > 0 {
		return c.execute(spanCtx, req, res[0])
	} else {
		return c.execute(spanCtx, req, nil)
	}
}

func (c *client) execute(ctx context.Context, req ex.Request, data any) error {

	var err error
	var retry bool

	for i, interval := range c.Backoff {
		time.Sleep(time.Duration(interval) * time.Second)

		c.Logger.Infof(">>> %+v", req)

		span, spanCtx := c.Tracer.StartSpan(ctx, "exec", ex.SpanTag{Key: "attempt", Value: i})
		defer span.Finish()

		if retry, err = c.Executor.Execute(spanCtx, req, data); !retry {
			break
		}

		if next := i + 1; next < len(c.Backoff) {
			c.Logger.Infof(">>> %+v [failed] retry in %ds", req, c.Backoff[next])
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
