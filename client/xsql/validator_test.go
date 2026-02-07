package xsql_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/reverted/ex"
	"github.com/reverted/ex/client/xsql"
)

type Validator interface {
	Validate(ex.Command, map[string]string) error
}

var _ = Describe("Validator", func() {

	var (
		err error

		req  ex.Command
		cols map[string]string

		validator Validator
	)

	BeforeEach(func() {

		cols = map[string]string{
			"id":   "INTEGER",
			"name": "VARCHAR(160)",
		}
	})

	JustBeforeEach(func() {
		err = validator.Validate(req, cols)
	})

	Context("when the builtin patterns are provided", func() {

		BeforeEach(func() {
			validator = xsql.NewValidator(newLogger(),
				xsql.WithPermittedColumnPatternAlias(),
				xsql.WithPermittedColumnPatternJsonPath(),
				xsql.WithPermittedColumnPatternRandom(),
			)
		})

		Context("when the resource is malformed", func() {
			BeforeEach(func() {
				req = ex.Query("invalid-resource")
			})

			It("errors", func() {
				Expect(err).To(HaveOccurred())
			})
		})

		Context("when base column does not exist", func() {
			BeforeEach(func() {
				req = ex.Query("resources", ex.Where{"invalid_column": "some-value"})
			})

			It("errors", func() {
				Expect(err).To(HaveOccurred())
			})
		})

		Context("when ordering by column that doesn't exist", func() {
			BeforeEach(func() {
				req = ex.Query("resources", ex.Order("invalid"))
			})

			It("errors", func() {
				Expect(err).To(HaveOccurred())
			})
		})

		Context("when partitioning by column that doesn't exist", func() {
			BeforeEach(func() {
				req = ex.Query("resources", ex.PartitionBy("invalid"))
			})

			It("errors", func() {
				Expect(err).To(HaveOccurred())
			})
		})

		Context("when partitioning by valid column", func() {
			BeforeEach(func() {
				req = ex.Query("resources", ex.PartitionBy("id"))
			})

			It("succeeds", func() {
				Expect(err).NotTo(HaveOccurred())
			})
		})

		Context("when partitioning by multiple valid columns", func() {
			BeforeEach(func() {
				req = ex.Query("resources", ex.PartitionBy("id", "name"))
			})

			It("succeeds", func() {
				Expect(err).NotTo(HaveOccurred())
			})
		})

		Context("when partitioning by mix of valid and invalid columns", func() {
			BeforeEach(func() {
				req = ex.Query("resources", ex.PartitionBy("id", "invalid"))
			})

			It("errors", func() {
				Expect(err).To(HaveOccurred())
			})
		})

		Context("when partitioning by json path on valid base column", func() {
			BeforeEach(func() {
				req = ex.Query("resources", ex.PartitionBy("id->>'key'"))
			})

			It("succeeds", func() {
				Expect(err).NotTo(HaveOccurred())
			})
		})

		Context("when partitioning by nested json path", func() {
			BeforeEach(func() {
				req = ex.Query("resources", ex.PartitionBy("id->data->>'user_id'"))
			})

			It("succeeds", func() {
				Expect(err).NotTo(HaveOccurred())
			})
		})

		Context("when partitioning by json path on invalid base column", func() {
			BeforeEach(func() {
				req = ex.Query("resources", ex.PartitionBy("invalid->>'key'"))
			})

			It("errors", func() {
				Expect(err).To(HaveOccurred())
			})
		})

		Context("when partitioning by multiple fields including json paths", func() {
			BeforeEach(func() {
				req = ex.Query("resources", ex.PartitionBy("id", "name->>'category'"))
			})

			It("succeeds", func() {
				Expect(err).NotTo(HaveOccurred())
			})
		})

		Context("when partitioning by json path with invalid syntax", func() {
			BeforeEach(func() {
				req = ex.Query("resources", ex.PartitionBy("id->>>'key'"))
			})

			It("errors", func() {
				Expect(err).To(HaveOccurred())
			})
		})

		Context("when querying a json column with invalid base path", func() {
			BeforeEach(func() {
				req = ex.Query("resources", ex.Where{"invalid_base->>'key'": "some-value"})
			})

			It("errors", func() {
				Expect(err).To(HaveOccurred())
			})
		})

		Context("when querying a valid base with invalid json path", func() {
			BeforeEach(func() {
				req = ex.Query("resources", ex.Where{"id->>>'key'": "some-value"})
			})

			It("errors", func() {
				Expect(err).To(HaveOccurred())
			})
		})

		Context("when querying a valid base with invalid json path", func() {
			BeforeEach(func() {
				req = ex.Query("resources", ex.Where{"id-->>'key'": "some-value"})
			})

			It("errors", func() {
				Expect(err).To(HaveOccurred())
			})
		})

		Context("when querying a base column", func() {
			BeforeEach(func() {
				req = ex.Query("resources", ex.Where{"id": "some-value"})
			})

			It("succeeds", func() {
				Expect(err).NotTo(HaveOccurred())
			})
		})

		Context("when querying a json path on base column with json segment", func() {
			BeforeEach(func() {
				req = ex.Query("resources", ex.Where{"id->'key'": "some-value"})
			})

			It("succeeds", func() {
				Expect(err).NotTo(HaveOccurred())
			})
		})

		Context("when querying a json path on base column with nested json segment", func() {
			BeforeEach(func() {
				req = ex.Query("resources", ex.Where{"id->data->'key'": "some-value"})
			})

			It("succeeds", func() {
				Expect(err).NotTo(HaveOccurred())
			})
		})

		Context("when querying a json path on base column with text segment", func() {
			BeforeEach(func() {
				req = ex.Query("resources", ex.Where{"id->>'key'": "some-value"})
			})

			It("succeeds", func() {
				Expect(err).NotTo(HaveOccurred())
			})
		})

		Context("when querying a json path on base column with nested text segment", func() {
			BeforeEach(func() {
				req = ex.Query("resources", ex.Where{"id->data->>'key'": "some-value"})
			})

			It("succeeds", func() {
				Expect(err).NotTo(HaveOccurred())
			})
		})

		Context("when querying an aliased based column", func() {
			BeforeEach(func() {
				req = ex.Query("resources", ex.Where{"id as permitted": "some-value"})
			})

			It("succeeds", func() {
				Expect(err).NotTo(HaveOccurred())
			})
		})

		Context("when querying a RANDOM() column", func() {
			BeforeEach(func() {
				req = ex.Query("resources", ex.Where{"RANDOM() AS random": "some-value"})
			})

			It("succeeds", func() {
				Expect(err).NotTo(HaveOccurred())
			})
		})
	})

	Context("when querying a permitted column", func() {
		BeforeEach(func() {
			validator = xsql.NewValidator(newLogger(),
				xsql.WithPermittedColumnPattern(`^permitted_column$`),
			)
			req = ex.Query("resources", ex.Where{"permitted_column": "some-value"})
		})

		It("succeeds", func() {
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Context("when querying a permitted column as alias", func() {
		BeforeEach(func() {
			validator = xsql.NewValidator(newLogger(),
				xsql.WithPermittedColumnPatternAlias(),
				xsql.WithPermittedColumnPattern(`^permitted_column$`),
			)
			req = ex.Query("resources", ex.Where{"permitted_column as permitted": "some-value"})
		})

		It("succeeds", func() {
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Context("when querying a permitted column as alias but alias is not enabled", func() {
		BeforeEach(func() {
			validator = xsql.NewValidator(newLogger(),
				xsql.WithPermittedColumnPattern(`^permitted_column$`),
			)
			req = ex.Query("resources", ex.Where{"permitted_column as permitted": "some-value"})
		})

		It("errors", func() {
			Expect(err).To(HaveOccurred())
		})
	})

	Context("when querying a permitted column as json path and alias", func() {
		BeforeEach(func() {
			validator = xsql.NewValidator(newLogger(),
				xsql.WithPermittedColumnPatternAlias(),
				xsql.WithPermittedColumnPatternJsonPath(),
				xsql.WithPermittedColumnPattern(`^permitted_column$`),
			)
			req = ex.Query("resources", ex.Where{"permitted_column->>'blah' as permitted": "some-value"})
		})

		It("succeeds", func() {
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Context("when querying a permitted column as json path and alias but json path is not enabled", func() {
		BeforeEach(func() {
			validator = xsql.NewValidator(newLogger(),
				xsql.WithPermittedColumnPatternAlias(),
				xsql.WithPermittedColumnPattern(`^permitted_column$`),
			)
			req = ex.Query("resources", ex.Where{"permitted_column->>'blah' as permitted": "some-value"})
		})

		It("errors", func() {
			Expect(err).To(HaveOccurred())
		})
	})
})
