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

	"github.com/silverswords/scheduler/pkg/schedule"
	"github.com/silverswords/scheduler/pkg/task"
)

var (
	errEmptyName     = errors.New("no name")
	errEmptySchedule = errors.New("no schedule")
)

type Config interface {
	IsSame(Config) (bool, error)
	NewTask() (string, task.Task)
	NewRemoteTask(s schedule.Schedule) task.Task
	NewSchedule() (schedule.Schedule, error)
}

type config struct {
	Name     string         `yaml:"name"`
	Schedule ScheduleConfig `yaml:"schedule"`
	Jobs     struct {
		Env   map[string]string `yaml:"env,omitempty"`
		Steps []Step            `yaml:"steps,omitempty"`
	} `yaml:"jobs,omitempty"`

	Upload string `yaml:"upload,omitempty"`
}
type ScheduleConfig struct {
	Cron     string   `yaml:"cron,omitempty"`
	Type     string   `yaml:"type,omitempty"`
	Priority int      `yaml:"priority,omitempty"`
	Lables   []string `yaml:"lables,omitempty"`
}

func (s ScheduleConfig) IsEmapty() bool {
	return reflect.DeepEqual(s, ScheduleConfig{})
}

type Step struct {
	Name    string   `yaml:"name,omitempty"`
	Run     string   `yaml:"run,omitempty"`
	Lables  []string `yaml:"lables,omitempty"`
	Depends []string `yaml:"depends,omitempty"`
}

func Unmarshal(data []byte) (*config, error) {
	c := config{}
	if err := yaml.Unmarshal([]byte(data), &c); err != nil {
		log.Fatalf("error: %v", err)
		return nil, err
	}

	return &c, nil
}

func (c *config) NewTask() (string, task.Task) {
	return c.Name, task.New(func(ctx context.Context) error {
		var msg string
		log.Printf("task %s run start\n", c.Name)
		for _, step := range c.Jobs.Steps {
			select {
			case <-ctx.Done():
				return errors.New("task has be cancled")
			default:
			}

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
				return fmt.Errorf("new task failed: %v", err)
			}

			log.Print(string(output))
			msg += fmt.Sprintf("step [%s] run result: %s\n", step.Name, string(output))
		}
		msg += fmt.Sprintf("task [%s] run finished\n", c.Name)
		return nil
	})
}

func (c *config) NewRemoteTask(s schedule.Schedule) task.Task {
	return &task.RemoteTask{
		Name:      c.Name,
		StartTime: s.Next(),
		Priority:  c.Schedule.Priority,
		Lables:    c.Schedule.Lables,
	}
}

func (c *config) NewSchedule() (s schedule.Schedule, err error) {
	switch c.Schedule.Type {
	case "once":
		s = schedule.NewOnceSchedule(c.Name)
	case "cron":
		s, err = schedule.NewCronSchedule(c.Name, c.Schedule.Cron)
		if err != nil {
			return nil, err
		}
	default:
		return nil, errors.New("error schedule type")
	}

	return
}

func (c *config) IsSame(newConfig Config) (bool, error) {
	old, err := yaml.Marshal(c)
	if err != nil {
		return false, err
	}

	config, ok := newConfig.(*config)
	if !ok {
		return false, errors.New("not the same type")
	}

	new, err := yaml.Marshal(config)
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

	return nil
}
