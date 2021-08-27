package config

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os/exec"
	"reflect"

	"gopkg.in/yaml.v2"

	"github.com/silverswords/scheduler/pkg/message"
	"github.com/silverswords/scheduler/pkg/schedule"
	"github.com/silverswords/scheduler/pkg/task"
)

var (
	errEmptyName     = errors.New("no name")
	errEmptySchedule = errors.New("no schedule")
	errEmptyJobs     = errors.New("no jobs")
)

type scheduleType int

const (
	once scheduleType = iota
	cron
)

type Config interface {
	IsSame(*Config) (bool, error)
	NewTask() (string, task.Task)
	NewSchedule() (schedule.Schedule, error)
}

type config struct {
	Name     string
	Schedule Schedule
	Jobs     Jobs
}
type Schedule struct {
	Cron string
	Type string
}

func (s Schedule) IsEmapty() bool {
	return reflect.DeepEqual(s, Schedule{})
}

type Jobs struct {
	Env   map[string]string
	Steps []Step
}

type Step struct {
	Name string
	Run  string
}

func (j Jobs) IsEmpty() bool {
	return reflect.DeepEqual(j, Jobs{})
}

func Unmarshal(data []byte) (Config, error) {
	c := config{}
	if err := yaml.Unmarshal([]byte(data), &c); err != nil {
		log.Fatalf("error: %v", err)
		return nil, err
	}

	return &c, nil
}

func (c *config) NewTask() (string, task.Task) {
	return c.Name, task.TaskFunc(func(ctx context.Context) error {
		var msg string

		for _, step := range c.Jobs.Steps {
			cmd := exec.CommandContext(ctx, "bash", "-c", step.Run)
			for k, v := range c.Jobs.Env {
				cmd.Env = append(cmd.Env, fmt.Sprintf("%s=%s", k, v))
			}
			output, err := cmd.Output()
			if err != nil {
				log.Println(err)
			}

			log.Println(string(output))
			msg += fmt.Sprintf("step %s run result: %s\n", step.Name, string(output))
		}

		log.Println("task run finished")
		summary := fmt.Sprintf("task %s run finished", c.Name)
		err := message.Push(summary, msg)
		if err != nil {
			return err
		}

		return nil
	})
}

func (c *config) NewSchedule() (schedule.Schedule, error) {
	switch c.Type() {
	case once:
		return schedule.NewOnceSchedule(c.Name), nil
	case cron:
		return schedule.NewCronSchedule(c.Name, c.Schedule.Cron)
	}

	return nil, errors.New("error schedule type")
}

func (c *config) Type() scheduleType {
	switch c.Schedule.Type {
	case "once":
		return once
	case "cron":
		return cron
	}

	return -1
}

func (c *config) IsSame(newConfig *Config) (bool, error) {
	old, err := yaml.Marshal(c)
	if err != nil {
		return false, err
	}

	new, err := yaml.Marshal(newConfig)
	if err != nil {
		return false, err
	}

	return string(new) == string(old), nil
}

func Validate(data []byte) error {
	c := config{}
	if err := yaml.Unmarshal(data, &c); err != nil {
		return err
	}

	if c.Name == "" {
		return errEmptyName
	}
	if c.Schedule.IsEmapty() {
		return errEmptySchedule
	}

	if c.Jobs.IsEmpty() {
		return errEmptyJobs
	}

	return nil
}
