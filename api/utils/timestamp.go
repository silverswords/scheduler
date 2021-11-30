package utils

import "time"

func FromTime(t time.Time) *Timestamp {
	return &Timestamp{
		Seconds: t.Unix(),
		Nanos:   int32(t.Nanosecond()),
	}
}

func (ts *Timestamp) ToTime() time.Time {
	return time.Unix(ts.Seconds, int64(ts.Nanos))
}
