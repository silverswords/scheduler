package config

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os/exec"
	"reflect"
	"strings"

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

type Config interface {
	IsSame(*Config) (bool, error)
	NewTask() (string, task.Task)
	NewSchedule() (schedule.Schedule, error)
	SetTag(string)
	GetTag() string
}

type config struct {
	Name     string
	Schedule Schedule
	Jobs     Jobs
	Upload   string
	// in order to reload distinct once type task
	// is a enum type: {"new", "old"}
	Tag string
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

func (c *config) SetTag(tag string) {
	c.Tag = tag
}

func (c *config) GetTag() string {
	return c.Tag
}

func (c *config) NewTask() (string, task.Task) {
	return c.Name, task.TaskFunc(func(ctx context.Context) error {
		var msg string
		log.Printf("task %s run start\n", c.Name)
		for _, step := range c.Jobs.Steps {
			if c.Upload != "" {
				step.Run = strings.Trim(step.Run, "./")
				step.Run = strings.ReplaceAll(step.Run, c.Upload, "./files/"+c.Upload)
			}
			cmd := exec.CommandContext(ctx, "bash", "-c", step.Run)
			for k, v := range c.Jobs.Env {
				cmd.Env = append(cmd.Env, fmt.Sprintf("%s=%s", k, v))
			}
			output, err := cmd.Output()
			if err != nil {
				log.Printf("new task failed: %v", err)
			}

			log.Print(string(output))
			msg += fmt.Sprintf("step %s run result: %s\n", step.Name, string(output))
		}

		log.Printf("task %s run finished\n", c.Name)
		summary := fmt.Sprintf("task %s run finished", c.Name)
		err := message.Push(summary, msg)
		if err != nil {
			return err
		}

		return nil
	})
}

func (c *config) NewSchedule() (schedule.Schedule, error) {
	switch c.Schedule.Type {
	case "once":
		return schedule.NewOnceSchedule(c.Name, "once"), nil
	case "cron":
		return schedule.NewCronSchedule(c.Name, c.Schedule.Cron, "cron")
	}

	return nil, errors.New("error schedule type")
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
