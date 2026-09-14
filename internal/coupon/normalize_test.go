package coupon

import "testing"

func TestNormalize(t *testing.T) {
	cases := []struct {
		in     string
		want   string
		wantOK bool
	}{
		{"HAPPYHRS", "HAPPYHRS", true},
		{"FIFTYOFF", "FIFTYOFF", true},
		{"ABCDEFGH12", "ABCDEFGH12", true}, // 10 chars, upper bound
		{"  HAPPYHRS  ", "HAPPYHRS", true}, // surrounding whitespace trimmed
		{"HAPPYHRS\r", "HAPPYHRS", true},   // CRLF tolerance
		{"1234567", "", false},             // 7 chars — too short
		{"ABCDEFGH123", "", false},         // 11 chars — too long
		{"", "", false},                    // empty
		{"happyhrs", "", false},            // lowercase not in alphabet
		{"HAPPY HRS", "", false},           // inner space
		{"HÄPPYHRS", "", false},            // multibyte: 8 runes, 9 bytes, bad byte
		{"HAPPY-HR", "", false},            // punctuation
		{"HAPPYHR\x00", "", false},         // NUL can never be part of a code
		{"OVER9000", "OVER9000", true},     // digits allowed
		{"\tFIFTYOFF\n", "FIFTYOFF", true}, // tabs/newlines trimmed
		{"          ", "", false},          // whitespace only
	}
	for _, tc := range cases {
		got, ok := Normalize(tc.in)
		if ok != tc.wantOK || got != tc.want {
			t.Errorf("Normalize(%q) = (%q, %v), want (%q, %v)", tc.in, got, ok, tc.want, tc.wantOK)
		}
	}
}

func TestFNV1a64Deterministic(t *testing.T) {
	if fnv1a64("HAPPYHRS") != fnv1a64("HAPPYHRS") {
		t.Fatal("hash must be deterministic")
	}
	if fnv1a64("HAPPYHRS") == fnv1a64("MOODYHRS") {
		t.Fatal("distinct codes should hash differently (sanity)")
	}
}
