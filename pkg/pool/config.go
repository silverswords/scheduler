package pool

import (
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

func buildStateError(got stepState, expect ...stepState) error {
	if len(expect) == 1 {
		return fmt.Errorf("wrong step state, expect %s, got %s", expect[0], got)
	}
	return fmt.Errorf("wrong step state, expect %v, got %s", expect, got)
}

type stepState string

const pendding stepState = "pendding"
const ready stepState = "ready"
const running stepState = "running"
const failed stepState = "failed"
const completed stepState = "completed"

type step struct {
	*config.Step
	c *runningConfig

	state         stepState
	wait          map[string]struct{}
	next          map[string]struct{}
	runningWorker string
}

func (s *step) newTask() (task.Task, error) {
	if s.state != pendding && s.state != failed {
		return nil, buildStateError(s.state, pendding, failed)
	}

	s.state = ready
	return &task.RemoteTask{
		Name: strings.Join([]string{strconv.FormatInt(s.c.startTime.UnixMicro(), 10), s.c.name, s.Name}, nameSeparator),
	}, nil
}

func (s *step) start(workerName string) error {
	if s.state != ready {
		return buildStateError(s.state, ready)
	}

	s.runningWorker, s.state = workerName, running
	return nil
}

func (s *step) complete() error {
	if s.state != running {
		return buildStateError(s.state, running)
	}

	s.state = completed
	return nil
}

func (s *step) fail() error {
	if s.state != running {
		return buildStateError(s.state, running)
	}

	s.state = failed
	return nil
}

type configHeap []*runningConfig

func (h configHeap) Len() int { return len(h) }
func (h configHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h configHeap) Less(i, j int) bool {
	if h[i].startTime.IsZero() {
		return false
	}
	if h[j].startTime.IsZero() {
		return true
	}
	return h[i].startTime.Before(h[j].startTime)
}

func (h configHeap) First() *runningConfig {
	return h[0]
}

func (h configHeap) Push(config interface{}) {
	h = append(h, config.(*runningConfig))
}

func (h configHeap) Pop() interface{} {
	result := h[len(h)-1]
	h = h[:len(h)-1]
	return result
}

func (h configHeap) Search(t time.Time, name string) *runningConfig {
	i := sort.Search(len(h), func(i int) bool {
		return h[i].startTime.Equal(t) && h[i].name == name
	})
	return h[i]
}

type runningConfig struct {
	lock      sync.Mutex
	name      string
	tasks     map[string]*step
	startTime time.Time
}

func fromConfig(c *config.Config) *runningConfig {
	config := &runningConfig{
		name:      c.Name,
		tasks:     make(map[string]*step),
		startTime: time.Now(),
	}

	tasks := make(map[string]*step)
	for _, s := range c.Jobs.Steps {
		tasks[s.Name] = &step{
			Step:  s,
			c:     config,
			state: pendding,
			wait:  make(map[string]struct{}),
			next:  make(map[string]struct{}),
		}
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

	return avaliableTask, nil
}

func (c *runningConfig) Complete(complete string) ([]task.Task, error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	completeTask := c.tasks[complete]
	if err := completeTask.complete(); err != nil {
		return nil, err
	}

	avaliableTask := []string{}

	for task := range completeTask.next {
		delete(c.tasks[task].wait, complete)
		if len(c.tasks[task].wait) == 0 {
			avaliableTask = append(avaliableTask, task)
		}
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
