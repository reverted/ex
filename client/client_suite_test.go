package client_test

import (
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/reverted/ex"
	"github.com/reverted/ex/client"
	"github.com/reverted/ex/client/xhttp"
	"github.com/reverted/ex/client/xsql"
	"github.com/reverted/ex/server"
	"github.com/reverted/logger"
)

func TestClient(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Client Suite")
}

type Conn interface {
	xsql.Connection
	Close() error
}

var (
	db *database

	apiServer *httptest.Server
	apiClient *http.Client

	sqlConn    Conn
	sqlClient  client.Client
	httpClient client.Client
)

var _ = BeforeEach(func() {

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
	)

	sqlClient = client.New(
		logger,
		client.WithExecutor(sqlExecutor),
	)

	apiServer = httptest.NewServer(server.New(logger, sqlClient))
	apiClient = apiServer.Client()

	target, err := url.Parse(apiServer.URL + "/v1/")
	Expect(err).NotTo(HaveOccurred())

	httpExecutor := xhttp.NewExecutor(
		logger,
		xhttp.WithTarget(target),
		xhttp.WithClient(apiClient),
	)

	httpClient = client.New(
		logger,
		client.WithExecutor(httpExecutor),
	)
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
	name := "ex_client" + "_" + randomString()

	db, err := sql.Open("mysql", "tcp(localhost:3306)/")
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
	return "tcp(localhost:3306)/" + self.name
}

func newResource(id int, name string) resource {
	return resource{
		Id:   id,
		Name: name,
	}
}

var createResources = `CREATE TABLE resources (
  id            INTEGER         PRIMARY KEY AUTO_INCREMENT,
  name          VARCHAR(160)    NOT NULL,
  email         VARCHAR(160)    NOT NULL
)`

type resource struct {
	Id    int    `json:"id"`
	Name  string `json:"name"`
	Email string `json:"email"`
}

func (self *resource) Scan(rows *sql.Rows, cols ...string) error {
	var email sql.NullString
	if err := rows.Scan(&self.Id, &self.Name, &email); err != nil {
		return err
	}
	self.Email = email.String
	return nil
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
