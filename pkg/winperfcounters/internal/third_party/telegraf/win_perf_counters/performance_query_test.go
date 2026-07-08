//go:build windows

package win_perf_counters

import (
	"testing"
	"unicode/utf16"

	"github.com/stretchr/testify/require"
)

func TestUTF16PtrToString(t *testing.T) {
	t.Run("String 'Hello World'", func(t *testing.T) {
		testPtr := (*uint16)(&utf16.Encode([]rune("Hello World\000"))[0])
		strOut := UTF16PtrToString(testPtr)
		require.Equal(t, "Hello World", strOut)
	})

	t.Run("nil pointer", func(t *testing.T) {
		strOut := UTF16PtrToString(nil)
		require.Equal(t, "", strOut)
	})
}

func TestUTF16ToStringArray(t *testing.T) {
	testStr := "First String\000Second String\000Final String\000\000"
	testStrUTF16 := utf16.Encode([]rune(testStr))

	strs := UTF16ToStringArray(testStrUTF16)
	require.Equal(t, []string{
		"First String",
		"Second String",
		"Final String",
	}, strs)
}

func TestIsIgnorablePDHError(t *testing.T) {
	tests := []struct {
		name     string
		retCode  uint32
		expected bool
	}{
		{"PDH_INVALID_DATA", PDH_INVALID_DATA, true},
		{"PDH_NO_DATA", PDH_NO_DATA, true},
		{"PDH_CALC_NEGATIVE_DENOMINATOR", PDH_CALC_NEGATIVE_DENOMINATOR, true},
		{"ERROR_SUCCESS", ERROR_SUCCESS, false},
		{"PDH_MORE_DATA", PDH_MORE_DATA, false},
		{"PDH_CSTATUS_NO_INSTANCE", PDH_CSTATUS_NO_INSTANCE, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsIgnorablePDHError(tt.retCode); got != tt.expected {
				t.Errorf("IsIgnorablePDHError() = %v, want %v", got, tt.expected)
			}
		})
	}
}
