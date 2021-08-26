package config

import (
	"context"
	"fmt"
	"log"
	"os/exec"

	"gopkg.in/yaml.v2"

	"github.com/silverswords/scheduler/pkg/schedule"
	"github.com/silverswords/scheduler/pkg/task"
)

type Config interface {
	IsSame(*Config) (bool, error)
	NewTask() (string, task.Task)
	NewSchedule() (schedule.Schedule, error)
}

type config struct {
	Name     string
	Schedule struct {
		Cron string
	}
	Jobs struct {
		Env   map[string]string
		Steps []Step
	}
}
type Step struct {
	Name string
	Run  string
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
		}

		log.Println("task run finished")
		return nil
	})
}

func (c *config) NewSchedule() (schedule.Schedule, error) {
	return schedule.NewCronSchedule(c.Name, c.Schedule.Cron)
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

	return nil
}
