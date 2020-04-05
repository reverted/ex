package http

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"

	"golang.org/x/oauth2/clientcredentials"

	"github.com/reverted/ex"
)

type Formatter interface {
	Format(ex.Request) (*http.Request, error)
}

type opt func(*executor)

func FromEnv() opt {
	return func(self *executor) {

		config := clientcredentials.Config{
			ClientID:     os.Getenv("REVERTED_API_CLIENT_ID"),
			ClientSecret: os.Getenv("REVERTED_API_CLIENT_SECRET"),
			TokenURL:     os.Getenv("REVERTED_API_TOKEN_URL"),
			Scopes:       strings.Split(os.Getenv("REVERTED_API_SCOPE"), ","),
		}

		client := config.Client(context.Background())

		url, err := url.Parse(os.Getenv("REVERTED_API_URL"))
		if err != nil {
			self.Logger.Fatal(err)
		}

		With(client, url)(self)
	}
}

func With(client *http.Client, target *url.URL) opt {
	return func(self *executor) {
		WithFormatter(NewFormatter(target))(self)
		WithClient(client)(self)
	}
}

func WithFormatter(formatter Formatter) opt {
	return func(self *executor) {
		self.Formatter = formatter
	}
}

func WithClient(client *http.Client) opt {
	return func(self *executor) {
		self.Client = client
	}
}

func NewExecutorFromEnv(logger Logger) *executor {
	return NewExecutor(logger, FromEnv())
}

func NewExecutor(logger Logger, opts ...opt) *executor {

	executor := &executor{Logger: logger}

	for _, opt := range opts {
		opt(executor)
	}

	if executor.Formatter == nil {
		url, _ := url.Parse("http://localhost:8080")
		WithFormatter(NewFormatter(url))(executor)
	}

	if executor.Client == nil {
		WithClient(http.DefaultClient)(executor)
	}

	return executor
}

type executor struct {
	Logger
	Formatter
	*http.Client
}

func (self *executor) Close() error {
	return nil
}

func (self *executor) Execute(req ex.Request, data interface{}) (bool, error) {

	r, err := self.Formatter.Format(req)
	if err != nil {
		return false, err
	}

	return self.exec(r, data)
}

func (self *executor) exec(r *http.Request, data interface{}) (bool, error) {

	self.Logger.Infof(">>> %v", r.URL)

	resp, err := self.Client.Do(r)
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
