package xpg_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/reverted/ex"
	"github.com/reverted/ex/client/xsql"
	"github.com/reverted/ex/client/xsql/xpg"
)

var _ = Describe("Formatter", func() {

	var (
		err error

		cmd  ex.Command
		stmt ex.Statement

		formatter xsql.Formatter
	)

	BeforeEach(func() {
		formatter = xpg.NewFormatter()
	})

	JustBeforeEach(func() {
		stmt, err = formatter.Format(cmd)
	})

	Context("when the command is unknown", func() {
		BeforeEach(func() {
			cmd = ex.Command{Action: "some-action"}
		})

		It("errors", func() {
			Expect(err).To(HaveOccurred())
		})
	})

	Describe("QUERY", func() {
		BeforeEach(func() {
			cmd = ex.Query("resources",
				ex.Where{"key": "value"},
				ex.Order{"key"},
				ex.Limit{Arg: 1},
				ex.Offset{Arg: 10},
			)
		})

		It("formats the command", func() {
			Expect(stmt.Stmt).To(Equal("SELECT * FROM resources WHERE key = $1 ORDER BY key LIMIT 1 OFFSET 10"))
			Expect(stmt.Args).To(ConsistOf("value"))
		})

		Context("when the command has where args", func() {
			BeforeEach(func() {
				cmd = ex.Query("resources", ex.Where{"key": "value"})
			})

			It("formats the command", func() {
				Expect(stmt.Stmt).To(Equal("SELECT * FROM resources WHERE key = $1"))
				Expect(stmt.Args).To(ConsistOf("value"))
			})
		})

		Context("when the command has order", func() {
			BeforeEach(func() {
				cmd = ex.Query("resources", ex.Order{"key"})
			})

			It("formats the command", func() {
				Expect(stmt.Stmt).To(Equal("SELECT * FROM resources ORDER BY key"))
			})
		})

		Context("when the command has limit", func() {
			BeforeEach(func() {
				cmd = ex.Query("resources", ex.Limit{Arg: 1})
			})

			It("formats the command", func() {
				Expect(stmt.Stmt).To(Equal("SELECT * FROM resources LIMIT 1"))
			})
		})

		Context("when the command has offset", func() {
			BeforeEach(func() {
				cmd = ex.Query("resources", ex.Offset{Arg: 10})
			})

			It("formats the command", func() {
				Expect(stmt.Stmt).To(Equal("SELECT * FROM resources OFFSET 10"))
			})
		})
	})

	Describe("DELETE", func() {
		BeforeEach(func() {
			cmd = ex.Delete("resources",
				ex.Where{"key": "value"},
				ex.Order{"key"},
				ex.Limit{Arg: 1},
			)
		})

		It("formats the command", func() {
			Expect(stmt.Stmt).To(Equal("DELETE FROM resources WHERE key = $1 ORDER BY key LIMIT 1"))
			Expect(stmt.Args).To(ConsistOf("value"))
		})

		Context("when the command has where args", func() {
			BeforeEach(func() {
				cmd = ex.Delete("resources", ex.Where{"key": "value"})
			})

			It("formats the command", func() {
				Expect(stmt.Stmt).To(Equal("DELETE FROM resources WHERE key = $1"))
				Expect(stmt.Args).To(ConsistOf("value"))
			})
		})

		Context("when the command has order", func() {
			BeforeEach(func() {
				cmd = ex.Delete("resources", ex.Order{"key"})
			})

			It("formats the command", func() {
				Expect(stmt.Stmt).To(Equal("DELETE FROM resources ORDER BY key"))
			})
		})

		Context("when the command has limit", func() {
			BeforeEach(func() {
				cmd = ex.Delete("resources", ex.Limit{Arg: 1})
			})

			It("formats the command", func() {
				Expect(stmt.Stmt).To(Equal("DELETE FROM resources LIMIT 1"))
			})
		})
	})

	Describe("INSERT", func() {
		BeforeEach(func() {
			cmd = ex.Insert("resources",
				ex.Values{"key": "value"},
			)
		})

		It("formats the command", func() {
			Expect(stmt.Stmt).To(Equal("INSERT INTO resources SET key = $1"))
			Expect(stmt.Args).To(ConsistOf("value"))
		})

		Context("when the command has conflict update", func() {
			BeforeEach(func() {
				cmd = ex.Insert("resources",
					ex.Values{"key": "value"},
					ex.OnConflictUpdate{"key"},
				)
			})

			It("formats the command", func() {
				Expect(stmt.Stmt).To(Equal("INSERT INTO resources SET key = $1 ON CONFLICT DO UPDATE SET key = excluded.key"))
				Expect(stmt.Args).To(ConsistOf("value"))
			})
		})

		Context("when the command is wrapped in ex.Json", func() {
			BeforeEach(func() {
				cmd = ex.Insert("resources",
					ex.Values{"key": ex.Json{Arg: []string{"value1", "value2"}}},
				)
			})

			It("formats the command as json", func() {
				Expect(stmt.Stmt).To(Equal("INSERT INTO resources SET key = $1"))
				Expect(stmt.Args).To(ConsistOf("[\"value1\",\"value2\"]"))
			})
		})

		Context("when the command has a interface slice value", func() {
			BeforeEach(func() {
				cmd = ex.Insert("resources",
					ex.Values{"key": []interface{}{"value1", "value2"}},
				)
			})

			It("formats the command as json", func() {
				Expect(stmt.Stmt).To(Equal("INSERT INTO resources SET key = $1"))
				Expect(stmt.Args).To(ConsistOf("[\"value1\",\"value2\"]"))
			})
		})

		Context("when the command has a string slice value", func() {
			BeforeEach(func() {
				cmd = ex.Insert("resources",
					ex.Values{"key": []string{"value1", "value2"}},
				)
			})

			It("formats the command as json", func() {
				Expect(stmt.Stmt).To(Equal("INSERT INTO resources SET key = $1"))
				Expect(stmt.Args).To(ConsistOf("[\"value1\",\"value2\"]"))
			})
		})

		Context("when the command has a map string interface value", func() {
			BeforeEach(func() {
				cmd = ex.Insert("resources",
					ex.Values{"key": map[string]interface{}{"key": 0}},
				)
			})

			It("formats the command as json", func() {
				Expect(stmt.Stmt).To(Equal("INSERT INTO resources SET key = $1"))
				Expect(stmt.Args).To(ConsistOf("{\"key\":0}"))
			})
		})

		Context("when the command has a map string string value", func() {
			BeforeEach(func() {
				cmd = ex.Insert("resources",
					ex.Values{"key": map[string]string{"key": "value"}},
				)
			})

			It("formats the command as json", func() {
				Expect(stmt.Stmt).To(Equal("INSERT INTO resources SET key = $1"))
				Expect(stmt.Args).To(ConsistOf("{\"key\":\"value\"}"))
			})
		})

		Context("when the command has conflict ignore", func() {
			BeforeEach(func() {
				cmd = ex.Insert("resources",
					ex.Values{"key": "value"},
					ex.OnConflictIgnore("true"),
				)
			})

			It("formats the command", func() {
				Expect(stmt.Stmt).To(Equal("INSERT INTO resources SET key = $1 ON CONFLICT DO NOTHING"))
				Expect(stmt.Args).To(ConsistOf("value"))
			})
		})

		Context("when the command has conflict error", func() {
			BeforeEach(func() {
				cmd = ex.Insert("resources",
					ex.Values{"key": "value"},
					ex.OnConflictError("true"),
				)
			})

			It("formats the command", func() {
				Expect(stmt.Stmt).To(Equal("INSERT INTO resources SET key = $1"))
				Expect(stmt.Args).To(ConsistOf("value"))
			})
		})
	})

	Describe("UPDATE", func() {
		BeforeEach(func() {
			cmd = ex.Update("resources",
				ex.Values{"key1": "value1"},
				ex.Where{"key2": "value2"},
				ex.Order{"key"},
				ex.Limit{Arg: 1},
			)
		})

		It("formats the command", func() {
			Expect(stmt.Stmt).To(Equal("UPDATE resources SET key1 = $1 WHERE key2 = $2 ORDER BY key LIMIT 1"))
			Expect(stmt.Args).To(ConsistOf("value1", "value2"))
		})

		Context("when the command has values", func() {
			BeforeEach(func() {
				cmd = ex.Update("resources", ex.Values{"key": "value"})
			})

			It("formats the command", func() {
				Expect(stmt.Stmt).To(Equal("UPDATE resources SET key = $1"))
				Expect(stmt.Args).To(ConsistOf("value"))
			})
		})

		Context("when the command has where args", func() {
			BeforeEach(func() {
				cmd = ex.Update("resources", ex.Where{"key": "value"})
			})

			It("formats the command", func() {
				Expect(stmt.Stmt).To(Equal("UPDATE resources WHERE key = $1"))
				Expect(stmt.Args).To(ConsistOf("value"))
			})
		})

		Context("when the command has order", func() {
			BeforeEach(func() {
				cmd = ex.Update("resources", ex.Order{"key"})
			})

			It("formats the command", func() {
				Expect(stmt.Stmt).To(Equal("UPDATE resources ORDER BY key"))
			})
		})

		Context("when the command has limit", func() {
			BeforeEach(func() {
				cmd = ex.Update("resources", ex.Limit{Arg: 1})
			})

			It("formats the command", func() {
				Expect(stmt.Stmt).To(Equal("UPDATE resources LIMIT 1"))
			})
		})
	})
})
