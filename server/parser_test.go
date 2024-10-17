package server_test

import (
	"bytes"
	"io"
	"net/http"
	"net/url"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/reverted/ex"
	"github.com/reverted/ex/server"
)

var _ = Describe("Parser", func() {
	var (
		err    error
		parser server.Parser

		req *http.Request
		res ex.Request
	)

	BeforeEach(func() {
		parser = server.NewParser()

		req, err = http.NewRequest("HEAD", "/v1/resources", bytes.NewBufferString(``))
		Expect(err).NotTo(HaveOccurred())
	})

	JustBeforeEach(func() {
		res, err = parser.Parse(req)
	})

	Context("when the method is not supported", func() {
		BeforeEach(func() {
			req.Method = "HEAD"
		})

		It("errors", func() {
			Expect(err).To(HaveOccurred())
		})
	})

	Describe("GET", func() {
		BeforeEach(func() {
			req.Method = "GET"
		})

		It("parses the request", func() {
			Expect(res).To(Equal(ex.Query("resources")))
		})

		Context("when the request has where args", func() {
			BeforeEach(func() {
				req.URL.RawQuery = "key=value"
			})

			It("parses the request", func() {
				Expect(res).To(Equal(ex.Query("resources", ex.Where{"key": "value"})))
			})
		})

		Context("when the request has columns", func() {
			BeforeEach(func() {
				req.Header.Add("X-Columns", "name")
			})

			It("parses the request", func() {
				Expect(res).To(Equal(ex.Query("resources", ex.Columns("name"))))
			})
		})

		Context("when the request has order", func() {
			BeforeEach(func() {
				req.Header.Add("X-Group-By", "name")
			})

			It("parses the request", func() {
				Expect(res).To(Equal(ex.Query("resources", ex.GroupBy("name"))))
			})
		})

		Context("when the request has order", func() {
			BeforeEach(func() {
				req.Header.Add("X-Order-By", "name")
			})

			It("parses the request", func() {
				Expect(res).To(Equal(ex.Query("resources", ex.Order("name"))))
			})
		})

		Context("when the request has limit", func() {
			BeforeEach(func() {
				req.Header.Add("X-Limit", "1")
			})

			It("parses the request", func() {
				Expect(res).To(Equal(ex.Query("resources", ex.Limit(1))))
			})
		})

		Context("when the request has an invalid limit", func() {
			BeforeEach(func() {
				req.Header.Add("X-Limit", "value")
			})

			It("errors", func() {
				Expect(err).To(HaveOccurred())
			})
		})

		Context("when the request has offset", func() {
			BeforeEach(func() {
				req.Header.Add("X-Offset", "10")
			})

			It("parses the request", func() {
				Expect(res).To(Equal(ex.Query("resources", ex.Offset(10))))
			})
		})

		Context("when the request has an invalid offset", func() {
			BeforeEach(func() {
				req.Header.Add("X-Offset", "value")
			})

			It("errors", func() {
				Expect(err).To(HaveOccurred())
			})
		})
	})

	Describe("DELETE", func() {
		BeforeEach(func() {
			req.Method = "DELETE"
		})

		Context("when the request has where args", func() {
			BeforeEach(func() {
				req.URL.RawQuery = "key=value"
			})

			It("parses the request", func() {
				Expect(res).To(Equal(ex.Delete("resources", ex.Where{"key": "value"})))
			})
		})

		Context("when the request has order", func() {
			BeforeEach(func() {
				req.Header.Add("X-Order-By", "name")
			})

			It("parses the request", func() {
				Expect(res).To(Equal(ex.Delete("resources", ex.Order("name"))))
			})
		})

		Context("when the request has limit", func() {
			BeforeEach(func() {
				req.Header.Add("X-Limit", "1")
			})

			It("parses the request", func() {
				Expect(res).To(Equal(ex.Delete("resources", ex.Limit(1))))
			})
		})

		Context("when the request has an invalid limit", func() {
			BeforeEach(func() {
				req.Header.Add("X-Limit", "value")
			})

			It("errors", func() {
				Expect(err).To(HaveOccurred())
			})
		})
	})

	Describe("POST", func() {
		BeforeEach(func() {
			req.Method = "POST"
		})

		Context("when the request has values", func() {
			BeforeEach(func() {
				req.Body = io.NopCloser(bytes.NewBufferString(`{"key": "value"}`))
			})

			It("parses the request", func() {
				Expect(res).To(Equal(ex.Insert("resources", ex.Values{"key": "value"})))
			})
		})

		Context("when the request has a conflict constraint", func() {
			BeforeEach(func() {
				req.Header.Add("X-On-Conflict-Constraint", "key1,key2")
			})

			It("parses the request", func() {
				Expect(res).To(Equal(ex.Insert("resources", ex.OnConflictConstraint("key1", "key2"))))
			})
		})

		Context("when the request has a conflict update", func() {
			BeforeEach(func() {
				req.Header.Add("X-On-Conflict-Update", "key1,key2")
			})

			It("parses the request", func() {
				Expect(res).To(Equal(ex.Insert("resources", ex.OnConflictUpdate("key1", "key2"))))
			})
		})

		Context("when the request has a conflict ignore", func() {
			BeforeEach(func() {
				req.Header.Add("X-On-Conflict-Ignore", "true")
			})

			It("parses the request", func() {
				Expect(res).To(Equal(ex.Insert("resources", ex.OnConflictIgnore("true"))))
			})
		})

		Context("when the request has a conflict error", func() {
			BeforeEach(func() {
				req.Header.Add("X-On-Conflict-Error", "true")
			})

			It("parses the request", func() {
				Expect(res).To(Equal(ex.Insert("resources", ex.OnConflictError("true"))))
			})
		})
	})

	Describe("PUT", func() {
		BeforeEach(func() {
			req.Method = "PUT"
		})

		Context("when the request has values", func() {
			BeforeEach(func() {
				req.Body = io.NopCloser(bytes.NewBufferString(`{"key": "value"}`))
			})

			It("parses the request", func() {
				Expect(res).To(Equal(ex.Update("resources", ex.Values{
					"key": "value",
				})))
			})
		})

		Context("when the request has where args", func() {
			BeforeEach(func() {
				req.URL.RawQuery = "key=value"
			})

			It("parses the request", func() {
				Expect(res).To(Equal(ex.Update("resources", ex.Where{"key": "value"})))
			})
		})

		Context("when the request has order", func() {
			BeforeEach(func() {
				req.Header.Add("X-Order-By", "name")
			})

			It("parses the request", func() {
				Expect(res).To(Equal(ex.Update("resources", ex.Order("name"))))
			})
		})

		Context("when the request has limit", func() {
			BeforeEach(func() {
				req.Header.Add("X-Limit", "1")
			})

			It("parses the request", func() {
				Expect(res).To(Equal(ex.Update("resources", ex.Limit(1))))
			})
		})

		Context("when the request has an invalid limit", func() {
			BeforeEach(func() {
				req.Header.Add("X-Limit", "value")
			})

			It("errors", func() {
				Expect(err).To(HaveOccurred())
			})
		})
	})

	Describe("Modifiers", func() {
		BeforeEach(func() {
			req.Method = "GET"

			values := url.Values{}
			values.Add("key-a:eq", "value")
			values.Add("key-b:not_eq", "value")
			values.Add("key-c:gt", "value")
			values.Add("key-d:gt_eq", "value")
			values.Add("key-e:lt", "value")
			values.Add("key-f:lt_eq", "value")
			values.Add("key-g:is", "value")
			values.Add("key-i:is_not", "value")
			values.Add("key-j:like", "value")
			values.Add("key-k:not_like", "value")
			values.Add("key-l:in", "value1,value2")
			values.Add("key-m:not_in", "value1,value2")
			values.Add("key-n:btwn", "value1,value2")
			values.Add("key-o:not_btwn", "value1,value2")
			req.URL.RawQuery = values.Encode()
		})

		It("parses the request", func() {
			cmd, ok := res.(ex.Command)
			Expect(ok).To(BeTrue())

			Expect(cmd.Where["key-a"]).To(Equal(ex.Eq("value")))
			Expect(cmd.Where["key-b"]).To(Equal(ex.NotEq("value")))
			Expect(cmd.Where["key-c"]).To(Equal(ex.Gt("value")))
			Expect(cmd.Where["key-d"]).To(Equal(ex.GtEq("value")))
			Expect(cmd.Where["key-e"]).To(Equal(ex.Lt("value")))
			Expect(cmd.Where["key-f"]).To(Equal(ex.LtEq("value")))
			Expect(cmd.Where["key-g"]).To(Equal(ex.Is("value")))
			Expect(cmd.Where["key-i"]).To(Equal(ex.IsNot("value")))
			Expect(cmd.Where["key-j"]).To(Equal(ex.Like("value")))
			Expect(cmd.Where["key-k"]).To(Equal(ex.NotLike("value")))
			Expect(cmd.Where["key-l"]).To(Equal(ex.In("value1", "value2")))
			Expect(cmd.Where["key-m"]).To(Equal(ex.NotIn("value1", "value2")))
			Expect(cmd.Where["key-n"]).To(Equal(ex.Btwn("value1", "value2")))
			Expect(cmd.Where["key-o"]).To(Equal(ex.NotBtwn("value1", "value2")))
		})
	})
})
