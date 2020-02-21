package modifier_test

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestModifier(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Modifier Suite")
}
