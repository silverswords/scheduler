package pool

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/silverswords/scheduler/pkg/config"
	"github.com/silverswords/scheduler/pkg/task"
)

const nameSeparator = "-"

var errSetTwice = errors.New("can't not set state change hook twice")

var (
	stepStateChangeHook   func(s *step)
	configStateChangeHook func(c *runningConfig)
)

func SetStepStateChangeHook(f func(s *step)) error {
	if stepStateChangeHook == nil {
		stepStateChangeHook = f
		return nil
	}

	return errSetTwice
}

func SetConfigStateChangeHook(f func(c *runningConfig)) error {
	if configStateChangeHook == nil {
		configStateChangeHook = f
		return nil
	}

	return errSetTwice
}

func buildStateError(got stepState, expect ...stepState) error {
	if len(expect) == 1 {
		return fmt.Errorf("wrong step state, expect %s, got %s", expect[0], got)
	}
	return fmt.Errorf("wrong step state, expect %v, got %s", expect, got)
}

type stepState int

const (
	stepPendding stepState = iota
	stepReady
	stepRunning
	stepFailed
	stepCompleted
)

func (s stepState) String() string {
	switch s {
	case stepPendding:
		return "stepPendding"
	case stepReady:
		return "stepReady"
	case stepRunning:
		return "stepRunning"
	case stepFailed:
		return "stepFailed"
	case stepCompleted:
		return "stepCompleted"
	default:
		return "wrong state"
	}
}

type step struct {
	*config.Step
	c *runningConfig

	state             stepState
	wait              map[string]struct{}
	next              map[string]struct{}
	stepRunningWorker string
	retryTimes        int
}

func (s *step) newTask() (task.Task, error) {
	if s.state != stepPendding && s.state != stepFailed {
		return nil, buildStateError(s.state, stepPendding, stepFailed)
	}

	s.state = stepReady
	if stepStateChangeHook != nil {
		go stepStateChangeHook(s)
	}

	return &task.RemoteTask{
		Name: strings.Join([]string{strconv.FormatInt(s.c.StartTime.UnixMicro(), 10), s.c.name, s.Name}, nameSeparator),
	}, nil
}

func (s *step) start(workerName string) error {
	if s.state != stepReady {
		return buildStateError(s.state, stepReady)
	}

	s.stepRunningWorker, s.state = workerName, stepRunning

	if stepStateChangeHook != nil {
		go stepStateChangeHook(s)
	}

	return nil
}

func (s *step) complete() error {
	if s.state != stepRunning {
		return buildStateError(s.state, stepRunning)
	}

	s.state = stepCompleted

	if stepStateChangeHook != nil {
		go stepStateChangeHook(s)
	}
	return nil
}

func (s *step) fail() error {
	if s.state != stepRunning {
		return buildStateError(s.state, stepRunning)
	}

	s.state = stepFailed
	if stepStateChangeHook != nil {
		go stepStateChangeHook(s)
	}
	return nil
}

type configHeap []*runningConfig

func (h configHeap) Len() int { return len(h) }
func (h configHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h configHeap) Less(i, j int) bool {
	if h[i].StartTime.IsZero() {
		return false
	}
	if h[j].StartTime.IsZero() {
		return true
	}
	return h[i].StartTime.Before(h[j].StartTime)
}

func (h configHeap) First() *runningConfig {
	return h[0]
}

func (h configHeap) Push(config interface{}) {
	h = append(h, config.(*runningConfig))
}

func (h configHeap) Pop() (result interface{}) {
	result, h = h[len(h)-1], h[:len(h)-1]
	return
}

func (h configHeap) Search(t time.Time, name string) *runningConfig {
	i := sort.Search(len(h), func(i int) bool {
		return h[i].StartTime.Equal(t) && h[i].name == name
	})

	return h[i]
}

func (h configHeap) Remove(c *runningConfig) {
	i := sort.Search(len(h), func(i int) bool {
		return h[i].StartTime.Equal(c.StartTime) && h[i].name == c.name
	})

	h = append(h[0:i], h[i+1:]...)
}

type configState int

const (
	configPendding configState = iota
	configRunning
	configCompleted
	configFailed
)

func (s configState) String() string {
	switch s {
	case configPendding:
		return "pendding"
	case configRunning:
		return "running"
	case configCompleted:
		return "completed"
	case configFailed:
		return "failed"
	default:
		return "wrong state"
	}
}

type runningConfig struct {
	lock         sync.Mutex
	name         string
	tasks        map[string]*step
	StartTime    time.Time
	completedNum int

	state configState
}

func fromConfig(c *config.Config) *runningConfig {
	config := &runningConfig{
		name:      c.Name,
		tasks:     make(map[string]*step),
		StartTime: time.Now(),
		state:     configPendding,
	}

	tasks := make(map[string]*step)
	for _, s := range c.Jobs.Steps {
		tasks[s.Name] = &step{
			Step:  s,
			c:     config,
			state: stepPendding,
			wait:  make(map[string]struct{}),
			next:  make(map[string]struct{}),
		}
	}

	if configStateChangeHook != nil {
		configStateChangeHook(config)
	}

	return config
}

func (c *runningConfig) Graph() ([]task.Task, error) {
	avaliableTask := []task.Task{}

	for _, s := range c.tasks {
		if len(s.Depends) == 0 {
			t, err := s.newTask()
			if err != nil {
				return nil, err
			}
			avaliableTask = append(avaliableTask, t)
		}

		for _, depend := range s.Depends {
			if s.wait == nil {
				s.wait = make(map[string]struct{})
			}
			s.wait[depend] = struct{}{}

			if c.tasks[depend].next == nil {
				c.tasks[depend].next = make(map[string]struct{})
			}
			c.tasks[depend].next[s.Name] = struct{}{}
		}
	}

	c.state = configRunning
	if configStateChangeHook != nil {
		configStateChangeHook(c)
	}

	return avaliableTask, nil
}

func (c *runningConfig) Complete(complete string) ([]task.Task, error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	completeTask := c.tasks[complete]
	if err := completeTask.complete(); err != nil {
		return nil, err
	}
	c.completedNum++

	avaliableTask := []string{}

	for task := range completeTask.next {
		delete(c.tasks[task].wait, complete)
		if len(c.tasks[task].wait) == 0 {
			avaliableTask = append(avaliableTask, task)
		}
	}

	c.state = configCompleted
	if configStateChangeHook != nil {
		configStateChangeHook(c)
	}

	return c.newTask(avaliableTask)
}

func (c *runningConfig) newTask(avaliableTask []string) (result []task.Task, err error) {
	for _, taskName := range avaliableTask {
		t, err := c.tasks[taskName].newTask()
		if err != nil {
			return nil, err
		}
		result = append(result, t)
	}

	return
}

func (c *runningConfig) Fail(fail string) (task.Task, error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	failTask := c.tasks[fail]
	if err := failTask.fail(); err != nil {
		return nil, err
	}

	if failTask.Step.Retry > failTask.retryTimes {
		failTask.retryTimes++
		return failTask.newTask()
	}

	c.state = configFailed
	if configStateChangeHook != nil {
		configStateChangeHook(c)
	}

	return nil, nil
}
