package xhttp_test

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestXHTTP(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "XHTTP Suite")
}
