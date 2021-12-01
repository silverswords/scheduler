package utils

import (
	"testing"
	"time"
)

func TimestampEqual(a *Timestamp, b *Timestamp) bool {
	if a.Seconds == b.Seconds && a.Nanos == b.Nanos {
		return true
	}

	return false
}
func TestFromTime(t *testing.T) {
	type args struct {
		t time.Time
	}
	tests := []struct {
		name string
		args args
		want *Timestamp
	}{
		{
			name: "basic",
			args: args{
				t: time.Time{},
			},
			want: &Timestamp{Seconds: -62135596800, Nanos: 0},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := FromTime(tt.args.t); !TimestampEqual(got, tt.want) {
				t.Errorf("FromTime() = %v, want %v", got, tt.want)
			}
		})
	}
}
