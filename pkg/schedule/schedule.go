package schedule

import (
	"time"

	"github.com/robfig/cron/v3"
)

// Schedule determines when to execute the task
type Schedule interface {
	Name() string
	Next() time.Time
	Step()
	Kind() string
}

// PrioritySchedule provides priority on schedule
type PrioritySchedule interface {
	Schedule
	Priority() int
}

type prioritySchedule struct {
	Schedule
	priority int
}

// NewPrioritySchedule creates an instance of PrioritySchedule
func NewPrioritySchedule(schedule Schedule, priority int) PrioritySchedule {
	return &prioritySchedule{
		schedule,
		priority,
	}
}

// Priority returns the task priority
func (s *prioritySchedule) Priority() int {
	return s.priority
}

// Kind returns the kind of schedule
func (s *prioritySchedule) Kind() string {
	return "priority(" + s.Schedule.Kind() + ")"
}

var standardParser = cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)

type cronSchedule struct {
	name string
	s    cron.Schedule
	next time.Time
	prev time.Time
}

// NewCronSchedule creates cron scheduling unit
func NewCronSchedule(name, cronStr string) (Schedule, error) {
	s, err := standardParser.Parse(cronStr)
	if err != nil {
		return nil, err
	}

	return &cronSchedule{
		name: name,
		s:    s,
	}, nil
}

// Name returns the name of the task
func (s *cronSchedule) Name() string {
	return s.name
}

// Next returns the time of the next execution of the task
func (s *cronSchedule) Next() time.Time {
	if s.next.IsZero() {
		s.next = s.s.Next(time.Now())
	}

	return s.next
}

// Step markes the end of this execution and enter the next
func (s *cronSchedule) Step() {
	s.prev = s.next
	s.next = s.s.Next(s.prev)
}

// Kind returns the kind of schedule
func (s *cronSchedule) Kind() string {
	return "cron"
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
}

// NewOnceSchedule creates once scheduling unit
func NewOnceSchedule(name string) Schedule {
	return &onceSchedule{name: name, next: time.Now()}
}

// Name returns the name of the task
func (o *onceSchedule) Name() string {
	return o.name
}

// Next returns the time of the next execution of the task
func (o *onceSchedule) Next() time.Time {
	return o.next
}

// Step markes the end of this execution and enter the next
func (o *onceSchedule) Step() {
	o.next = time.Time{}
}

// Kind returns the kind of schedule
func (s *onceSchedule) Kind() string {
	return "once"
}
