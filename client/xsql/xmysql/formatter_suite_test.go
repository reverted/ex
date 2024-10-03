package xmysql_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestXSQL(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "XMysql Suite")
}
