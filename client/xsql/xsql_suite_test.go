package xsql_test

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestXSQL(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "XSQL Suite")
}
