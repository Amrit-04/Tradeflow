package domain_test

import (
	"testing"
	"tradeflow/internal/domain"
)

func TestParsePriceString(t *testing.T) {
	testCases := []struct {
		input    string
		expected domain.Price
		hasError bool
	}{
		{"150.25", 15025, false},
		{"150.5", 15050, false},
		{"150.00", 15000, false},
		{"150", 15000, false},
		{"0.05", 5, false},
		{"0.50", 50, false},
		{"0.00", 0, false},
		{"999.99", 99999, false},
		{"190.255", 19025, false}, // Truncates sub-cent to 2 decimal places
		{"-10.00", 0, true},
		{"abc", 0, true},
		{"", 0, true},
	}

	for _, tc := range testCases {
		res, err := domain.ParsePriceString(tc.input)
		if tc.hasError {
			if err == nil {
				t.Fatalf("expected error for input %q, got price %d", tc.input, res)
			}
		} else {
			if err != nil {
				t.Fatalf("unexpected error for input %q: %v", tc.input, err)
			}
			if res != tc.expected {
				t.Fatalf("input %q: expected price %d, got %d", tc.input, tc.expected, res)
			}
		}
	}
}
