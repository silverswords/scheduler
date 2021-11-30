package pool

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/silverswords/scheduler/pkg/config"
	"github.com/silverswords/scheduler/pkg/task"
)

type step struct {
	*config.Step

	c             *runningConfig
	completed     bool
	wait          map[string]struct{}
	next          map[string]struct{}
	runningWorker string
}

func (s *step) newTask() task.Task {
	return &task.RemoteTask{
		Name: strings.Join([]string{strconv.FormatInt(s.c.createdTime.UnixMicro(), 10), s.c.name, s.Name}, "-"),
	}
}

type runningConfig struct {
	lock        sync.Mutex
	name        string
	tasks       map[string]*step
	createdTime time.Time
}

func fromConfig(c *config.Config) *runningConfig {
	config := &runningConfig{
		name:        c.Name,
		tasks:       make(map[string]*step),
		createdTime: time.Now(),
	}

	tasks := make(map[string]*step)
	for _, s := range c.Jobs.Steps {
		tasks[s.Name] = &step{
			Step: s,
			c:    config,
			wait: make(map[string]struct{}),
			next: make(map[string]struct{}),
		}
	}

	return config
}

func (c *runningConfig) Graph() ([]task.Task, error) {
	avaliableTask := []task.Task{}

	for _, s := range c.tasks {
		if len(s.Depends) == 0 {
			avaliableTask = append(avaliableTask, s.newTask())
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

func (c *runningConfig) Complete(complete string) []task.Task {
	c.lock.Lock()
	defer c.lock.Unlock()
	completeTask := c.tasks[complete]
	completeTask.completed = true
	avaliableTask := []string{}
	fmt.Println(completeTask.next, avaliableTask)
	for task := range completeTask.next {
		delete(c.tasks[task].wait, complete)
		if len(c.tasks[task].wait) == 0 {
			avaliableTask = append(avaliableTask, task)
		}
		fmt.Println(c.tasks[task].wait, avaliableTask)
	}

	return c.newTask(avaliableTask)
}

func (c *runningConfig) newTask(avaliableTask []string) (result []task.Task) {
	for _, task := range avaliableTask {
		result = append(result, c.tasks[task].newTask())
	}

	return
}
