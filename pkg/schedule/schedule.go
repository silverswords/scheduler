package schedule

import (
	"time"

	"github.com/robfig/cron/v3"
)

type Schedule interface {
	Name() string
	Next() time.Time
	Step()
}

var standardParser = cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)

type cronSchedule struct {
	name string
	s    cron.Schedule
	next time.Time
	prev time.Time
}

func NewCronSchedule(name string, cronStr string) (Schedule, error) {
	s, err := standardParser.Parse(cronStr)
	if err != nil {
		return nil, err
	}

	return &cronSchedule{
		name: name,
		s:    s,
	}, nil
}

func (s *cronSchedule) Name() string {
	return s.name
}

func (s *cronSchedule) Next() time.Time {
	if s.next.IsZero() {
		s.next = s.s.Next(time.Now())
	}

	return s.next
}

func (s *cronSchedule) Step() {
	s.prev = s.next
	s.next = s.s.Next(s.prev)
}

type ByTime []Schedule

func (s ByTime) Len() int      { return len(s) }
func (s ByTime) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s ByTime) Less(i, j int) bool {
	if s[i].Next().IsZero() {
		return false
	}
	if s[j].Next().IsZero() {
		return true
	}
	return s[i].Next().Before(s[j].Next())
}
