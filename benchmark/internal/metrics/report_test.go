package metrics

import "testing"

func TestFormatWallTime(t *testing.T) {
	tests := []struct {
		secs float64
		want string
	}{
		{0.97, "0.97s"},
		{12.345, "12.35s"},
		{59.99, "59.99s"},
		{60.0, "1m0s"},
		{3661, "1h1m1s"},
		{7320, "2h2m0s"},
	}
	for _, tt := range tests {
		got := formatWallTime(tt.secs)
		if got != tt.want {
			t.Errorf("formatWallTime(%v) = %q, want %q", tt.secs, got, tt.want)
		}
	}
}
