package xsql_test

import (
	"database/sql"
	"errors"
	"reflect"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/golang/mock/gomock"
	"github.com/reverted/ex/client/xsql"
	"github.com/reverted/ex/client/xsql/mocks"
)

var _ = Describe("Scanner", func() {

	var (
		err error

		mockCtrl *gomock.Controller
		mockRows *mocks.MockRows

		scanner xsql.Scanner
	)

	BeforeEach(func() {
		mockCtrl = gomock.NewController(GinkgoT())
		mockRows = mocks.NewMockRows(mockCtrl)
		mockRows.EXPECT().Next().Return(true).Times(1)

		scanner = xsql.NewScanner()
	})

	Describe("scanning into a scannable", func() {
		var res []*scannable

		BeforeEach(func() {
			res = []*scannable{}
		})

		JustBeforeEach(func() {
			err = scanner.Scan(mockRows, &res)
		})

		Context("when scanning fails", func() {
			BeforeEach(func() {
				mockRows.EXPECT().Scan(gomock.Any()).Return(errors.New("nope"))
			})

			It("errors", func() {
				Expect(err).To(HaveOccurred())
			})
		})

		Context("when scanning succeeds", func() {
			BeforeEach(func() {
				mockRows.EXPECT().Next().Return(false).Times(1)
				mockRows.EXPECT().Scan(gomock.Any()).Return(nil)
			})

			Context("when rows errors", func() {
				BeforeEach(func() {
					mockRows.EXPECT().Err().Return(errors.New("nope"))
				})

				It("errors", func() {
					Expect(err).To(HaveOccurred())
				})
			})

			Context("when rows does not error", func() {
				BeforeEach(func() {
					mockRows.EXPECT().Err().Return(nil)
				})

				It("succeeds", func() {
					Expect(err).NotTo(HaveOccurred())
				})
			})
		})
	})

	Describe("scanning into a map", func() {
		var res []map[string]interface{}

		BeforeEach(func() {
			res = []map[string]interface{}{}
		})

		JustBeforeEach(func() {
			err = scanner.Scan(mockRows, &res)
		})

		Context("when fetching columns fails", func() {
			BeforeEach(func() {
				mockRows.EXPECT().ColumnTypes().Return(nil, errors.New("nope"))
			})

			It("errors", func() {
				Expect(err).To(HaveOccurred())
			})
		})

		Context("when fetching columns succeeds", func() {
			BeforeEach(func() {
				mockRows.EXPECT().ColumnTypes().Return([]xsql.ColumnType{
					column{"key1", reflect.TypeOf(sql.NullString{}), "TEXT"},
					column{"key2", reflect.TypeOf(sql.NullString{}), "TEXT"},
				}, nil)
			})

			Context("when scanning fails", func() {
				BeforeEach(func() {
					mockRows.EXPECT().Scan(gomock.Any(), gomock.Any()).Return(errors.New("nope"))
				})

				It("errors", func() {
					Expect(err).To(HaveOccurred())
				})
			})

			Context("when scanning succeeds", func() {
				BeforeEach(func() {
					mockRows.EXPECT().Next().Return(false).Times(1)
					mockRows.EXPECT().Scan(gomock.Any(), gomock.Any()).Return(nil)
				})

				Context("when rows errors", func() {
					BeforeEach(func() {
						mockRows.EXPECT().Err().Return(errors.New("nope"))
					})

					It("errors", func() {
						Expect(err).To(HaveOccurred())
					})
				})

				Context("when rows does not error", func() {
					BeforeEach(func() {
						mockRows.EXPECT().Err().Return(nil)
					})

					It("succeeds", func() {
						Expect(err).NotTo(HaveOccurred())
					})

					It("scans the correct types", func() {
						Expect(res[0]["key1"]).To(BeAssignableToTypeOf(""))
						Expect(res[0]["key2"]).To(BeAssignableToTypeOf(""))
					})
				})
			})
		})
	})

	Describe("scanning with tags", func() {
		var res []*result

		BeforeEach(func() {
			res = []*result{}
		})

		JustBeforeEach(func() {
			err = scanner.Scan(mockRows, &res)
		})

		Context("when fetching columns fails", func() {
			BeforeEach(func() {
				mockRows.EXPECT().ColumnTypes().Return(nil, errors.New("nope"))
			})

			It("errors", func() {
				Expect(err).To(HaveOccurred())
			})
		})

		Context("when num columns != num fields", func() {
			BeforeEach(func() {
				mockRows.EXPECT().ColumnTypes().Return([]xsql.ColumnType{
					column{"key1", reflect.TypeOf(sql.NullString{}), "TEXT"},
					column{"key2", reflect.TypeOf(sql.NullString{}), "TEXT"},
					column{"key3", reflect.TypeOf(sql.NullString{}), "TEXT"},
				}, nil)
			})

			Context("when scanning fails", func() {
				BeforeEach(func() {
					mockRows.EXPECT().Scan(gomock.Any(), gomock.Any(), gomock.Any()).Return(errors.New("nope"))
				})

				It("errors", func() {
					Expect(err).To(HaveOccurred())
				})
			})

			Context("when scanning succeeds", func() {
				BeforeEach(func() {
					mockRows.EXPECT().Next().Return(false).Times(1)
					mockRows.EXPECT().Scan(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
				})

				Context("when rows fails", func() {
					BeforeEach(func() {
						mockRows.EXPECT().Err().Return(errors.New("nope"))
					})

					It("errors", func() {
						Expect(err).To(HaveOccurred())
					})
				})

				Context("when rows succeeds", func() {
					BeforeEach(func() {
						mockRows.EXPECT().Err().Return(nil)
					})

					It("succeeds", func() {
						Expect(err).NotTo(HaveOccurred())
					})
				})
			})
		})

		Context("when tag does not exist for column", func() {
			BeforeEach(func() {
				mockRows.EXPECT().ColumnTypes().Return([]xsql.ColumnType{
					column{"key1", reflect.TypeOf(sql.NullString{}), "TEXT"},
					column{"key4", reflect.TypeOf(sql.NullString{}), "TEXT"},
				}, nil)
			})

			Context("when scanning fails", func() {
				BeforeEach(func() {
					mockRows.EXPECT().Scan(gomock.Any(), gomock.Any()).Return(errors.New("nope"))
				})

				It("errors", func() {
					Expect(err).To(HaveOccurred())
				})
			})

			Context("when scanning succeeds", func() {
				BeforeEach(func() {
					mockRows.EXPECT().Scan(gomock.Any(), gomock.Any()).Return(nil)
				})

				It("errors", func() {
					Expect(err).To(HaveOccurred())
				})
			})
		})

		Context("when column is missing", func() {
			BeforeEach(func() {
				mockRows.EXPECT().ColumnTypes().Return([]xsql.ColumnType{
					column{"key1", reflect.TypeOf(sql.NullString{}), "TEXT"},
				}, nil)
			})

			Context("when scanning fails", func() {
				BeforeEach(func() {
					mockRows.EXPECT().Scan(gomock.Any()).Return(errors.New("nope"))
				})

				It("errors", func() {
					Expect(err).To(HaveOccurred())
				})
			})

			Context("when scanning succeeds", func() {
				BeforeEach(func() {
					mockRows.EXPECT().Scan(gomock.Any()).Return(nil)
				})

				It("errors", func() {
					Expect(err).To(HaveOccurred())
				})
			})
		})

		Context("when columns have type mismatch", func() {
			BeforeEach(func() {
				mockRows.EXPECT().ColumnTypes().Return([]xsql.ColumnType{
					column{"key1", reflect.TypeOf(sql.NullString{}), "TEXT"},
					column{"key2", reflect.TypeOf(sql.NullInt64{}), "DECIMAL"},
				}, nil)
			})

			Context("when scanning fails", func() {
				BeforeEach(func() {
					mockRows.EXPECT().Scan(gomock.Any(), gomock.Any()).Return(errors.New("nope"))
				})

				It("errors", func() {
					Expect(err).To(HaveOccurred())
				})
			})

			Context("when scanning succeeds", func() {
				BeforeEach(func() {
					mockRows.EXPECT().Scan(gomock.Any(), gomock.Any()).Return(nil)
				})

				It("errors", func() {
					Expect(err).To(HaveOccurred())
				})
			})
		})

		Context("when fetching columns succeeds", func() {
			BeforeEach(func() {
				mockRows.EXPECT().ColumnTypes().Return([]xsql.ColumnType{
					column{"key1", reflect.TypeOf(sql.NullString{}), "TEXT"},
					column{"key2", reflect.TypeOf(sql.NullString{}), "TEXT"},
				}, nil)
			})

			Context("when scanning fails", func() {
				BeforeEach(func() {
					mockRows.EXPECT().Scan(gomock.Any(), gomock.Any()).Return(errors.New("nope"))
				})

				It("errors", func() {
					Expect(err).To(HaveOccurred())
				})
			})

			Context("when scanning succeeds", func() {
				BeforeEach(func() {
					mockRows.EXPECT().Next().Return(false).Times(1)
					mockRows.EXPECT().Scan(gomock.Any(), gomock.Any()).Return(nil)
				})

				Context("when rows fails", func() {
					BeforeEach(func() {
						mockRows.EXPECT().Err().Return(errors.New("nope"))
					})

					It("errors", func() {
						Expect(err).To(HaveOccurred())
					})
				})

				Context("when rows succeeds", func() {
					BeforeEach(func() {
						mockRows.EXPECT().Err().Return(nil)
					})

					It("succeeds", func() {
						Expect(err).NotTo(HaveOccurred())
					})
				})
			})
		})
	})
})

type column struct {
	name     string
	scanType reflect.Type
	dbType   string
}

func (s column) Name() string {
	return s.name
}

func (s column) ScanType() reflect.Type {
	return s.scanType
}

func (s column) DatabaseTypeName() string {
	return s.dbType
}

type scannable struct{}

func (s scannable) Scan(rows xsql.Rows) error {
	return rows.Scan(s)
}

type result struct {
	Key1 string `json:"key1"`
	Key2 string `json:"key2"`
}
