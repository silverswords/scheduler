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

type HeapSet struct {
	schedules []Schedule
	m         map[string]int
}

func NewHeapSet() *HeapSet {
	return &HeapSet{
		schedules: make([]Schedule, 0),
		m:         make(map[string]int),
	}
}

func (s *HeapSet) Len() int { return len(s.schedules) }

func (s *HeapSet) Swap(i, j int) {
	s.schedules[i], s.schedules[j] = s.schedules[j], s.schedules[i]
	s.m[s.schedules[i].Name()], s.m[s.schedules[j].Name()] = s.m[s.schedules[j].Name()], s.m[s.schedules[i].Name()]
}

func (s *HeapSet) Less(i, j int) bool {
	if s.schedules[i].Next().IsZero() {
		return false
	}
	if s.schedules[j].Next().IsZero() {
		return true
	}
	return s.schedules[i].Next().Before(s.schedules[j].Next())
}

func (s *HeapSet) First() Schedule {
	return s.schedules[0]
}

func (s *HeapSet) Add(schedule Schedule) {
	if i, ok := s.m[schedule.Name()]; ok {
		s.schedules = append(s.schedules[0:i], s.schedules[i+1:len(s.schedules)]...)
		delete(s.m, schedule.Name())
	}

	s.schedules = append(s.schedules, schedule)
	s.m[schedule.Name()] = len(s.schedules) - 1
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
