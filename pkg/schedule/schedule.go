package schedule

import (
	"time"

	"github.com/robfig/cron/v3"
)

type Schedule interface {
	Name() string
	Next() time.Time
	Step()
	Kind() string
}

type PrioritySchedule interface {
	Schedule
	Priority() int
}

type prioritySchedule struct {
	Schedule
	priority int
}

func NewPrioritySchedule(schedule Schedule, priority int) PrioritySchedule {
	return &prioritySchedule{
		schedule,
		priority,
	}
}

func (s *prioritySchedule) Priority() int {
	return s.priority
}

var standardParser = cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)

type cronSchedule struct {
	name string
	s    cron.Schedule
	next time.Time
	prev time.Time
	kind string
}

func NewCronSchedule(name, cronStr string) (Schedule, error) {
	s, err := standardParser.Parse(cronStr)
	if err != nil {
		return nil, err
	}

	return &cronSchedule{
		name: name,
		s:    s,
		kind: "cron",
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

func (s *cronSchedule) Kind() string {
	return s.kind
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

type onceSchedule struct {
	name string
	next time.Time
	kind string
}

func NewOnceSchedule(name string) Schedule {
	return &onceSchedule{name: name, next: time.Now(), kind: "once"}
}

func (o *onceSchedule) Name() string {
	return o.name
}

func (o *onceSchedule) Next() time.Time {
	return o.next
}

func (o *onceSchedule) Step() {
	o.next = time.Time{}
}

func (s *onceSchedule) Kind() string {
	return s.kind
}
