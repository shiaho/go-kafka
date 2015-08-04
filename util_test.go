package kafka

import (
	"testing"
)

func TestStringArrayDiff(t *testing.T) {
	a := []string{"1", "2", "3", "4"}
	b := []string{"4"}
	res := stringArrayDiff(a, b)
	if len(res) != 3 {
		t.Errorf("len(res) should be 3, but get %d", len(res))
	}
}
