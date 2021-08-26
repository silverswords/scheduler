package config

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os/exec"
	"reflect"

	"github.com/silverswords/scheduler/pkg/task"
	"gopkg.in/yaml.v2"
)

type Config interface {
	New() task.Task
	IsSame(*Config) (bool, error)
}

type config struct {
	Name     string
	Schedule Schedule
	Jobs     Jobs
}
type Schedule struct {
	Cron string
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

func (c *config) New() task.Task {
	fmt.Println(c)
	return task.TaskFunc(func(ctx context.Context) error {
		for _, step := range c.Jobs.Steps {
			cmd := exec.CommandContext(ctx, "bash", "-c", step.Run)
			for k, v := range c.Jobs.Env {
				cmd.Env = append(cmd.Env, fmt.Sprintf("%s=%s", k, v))
			}
			output, err := cmd.Output()
			if err != nil {
				log.Println("cmd error")
			}
			log.Println(string(output))
		}

		return nil
	})
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

	if c.Schedule.IsEmapty() {
		return errors.New("no schedule")
	}

	if c.Jobs.IsEmpty() {
		return errors.New("no jobs")
	}

	return nil
}
