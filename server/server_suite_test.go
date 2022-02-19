package server_test

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/reverted/ex"
	"github.com/reverted/ex/client"
	"github.com/reverted/ex/client/xsql"
	"github.com/reverted/ex/server"
	"github.com/reverted/logger"
)

func TestServer(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Server Suite")
}

type Conn interface {
	xsql.Connection
	Close() error
}

var (
	db *database

	apiServer *httptest.Server
	apiClient *http.Client

	sqlConn   Conn
	sqlClient client.Client
)

var _ = BeforeEach(func() {

	tracer := noopTracer{}

	logger := logger.New("test",
		logger.Writer(GinkgoWriter),
		logger.Level(logger.Debug),
	)

	db = NewDatabase()

	sqlConn = xsql.NewConn("mysql", db.Uri())

	sqlExecutor := xsql.NewExecutor(
		logger,
		xsql.WithMysqlFormatter(),
		xsql.WithConnection(sqlConn),
		xsql.WithTracer(tracer),
	)

	sqlClient = client.New(
		logger,
		client.WithExecutor(sqlExecutor),
		client.WithTracer(tracer),
	)

	apiServer = httptest.NewServer(server.New(
		logger,
		sqlClient,
		server.WithTracer(tracer)),
	)
	apiClient = apiServer.Client()
})

var _ = AfterEach(func() {
	apiServer.Close()
	sqlConn.Close()
	db.Close()
})

type database struct {
	*sql.DB
	name string
}

func NewDatabase() *database {
	name := "ex_server" + "_" + randomString()

	db, err := sql.Open("mysql", connection())
	Expect(err).NotTo(HaveOccurred())

	_, err = db.Exec("CREATE DATABASE " + name)
	Expect(err).NotTo(HaveOccurred())

	return &database{db, name}
}

func (self *database) Close() {
	_, err := self.DB.Exec("DROP DATABASE " + self.name)
	Expect(err).NotTo(HaveOccurred())

	self.DB.Close()
}

func (self *database) Uri() string {
	return connection() + self.name
}

func connection() string {
	user, pass := os.Getenv("MYSQL_USER"), os.Getenv("MYSQL_PASS")

	if user != "" {
		return fmt.Sprintf("%s:%s@tcp(localhost:3306)/", user, pass)
	} else {
		return fmt.Sprintf("tcp(localhost:3306)/")
	}
}

func newResource(id int, name string) resource {
	return resource{
		Id:   id,
		Name: name,
	}
}

var createResources = `CREATE TABLE resources (
  id            INTEGER         PRIMARY KEY AUTO_INCREMENT,
  name          VARCHAR(160)    NOT NULL
)`

type resource struct {
	Id   int    `json:"id"`
	Name string `json:"name"`
}

func createResourcesTable() {
	err := sqlClient.Exec(ex.Exec(createResources))
	Expect(err).NotTo(HaveOccurred())
}

func queryResources() []resource {
	var data []resource
	err := sqlClient.Exec(ex.Query("resources"), &data)
	Expect(err).NotTo(HaveOccurred())
	return data
}

func insertResources(names ...string) {
	for _, name := range names {
		err := sqlClient.Exec(ex.Insert("resources", ex.Values{"name": name}))
		Expect(err).NotTo(HaveOccurred())
	}
}

func parseResources(resp *http.Response) []resource {
	var data []resource
	err := json.NewDecoder(resp.Body).Decode(&data)
	Expect(err).NotTo(HaveOccurred())
	return data
}

func randomString() string {
	randomBytes := make([]byte, 16)
	rand.Read(randomBytes)
	return hex.EncodeToString(randomBytes)
}

type noopSpan struct{}

func (self noopSpan) Finish() {}

type noopTracer struct{}

func (self noopTracer) StartSpan(ctx context.Context, name string, tags ...ex.SpanTag) (ex.Span, context.Context) {
	return noopSpan{}, ctx
}

func (self noopTracer) InjectSpan(ctx context.Context, r *http.Request) {
}

func (self noopTracer) ExtractSpan(r *http.Request, name string) (ex.Span, context.Context) {
	return noopSpan{}, r.Context()
}
