package config

import (
	"context"
	"fmt"
	"log"
	"os/exec"

	"github.com/silverswords/scheduler/pkg/task"
	"gopkg.in/yaml.v2"
)

type Config interface {
	New() task.Task
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
	err := yaml.Unmarshal([]byte(data), &c)
	if err != nil {
		log.Fatalf("error: %v", err)
	}

	return &c, nil
}

func (c *config) WithName(name string) *config {
	c.Name = name
	return c
}

func (c *config) WithSchedule(cron string) *config {
	c.Schedule.Cron = cron
	return c
}

func (c *config) WithEnv(env map[string]string) *config {
	c.Jobs.Env = env
	return c
}

func (c *config) WithSteps(steps ...Step) *config {
	c.Jobs.Steps = make([]Step, 0)
	for _, s := range steps {
		c.Jobs.Steps = append(c.Jobs.Steps, s)
	}

	return c
}

func (c *config) New() task.Task {
	return task.TaskFunc(func(ctx context.Context) error {
		for _, step := range c.Jobs.Steps {
			cmd := exec.CommandContext(ctx, "bash", "-c", step.Run)
			for k, v := range c.Jobs.Env {
				cmd.Env = append(cmd.Env, fmt.Sprintf("%s=%s", k, v))
			}
			output, err := cmd.Output()
			if err != nil {
				panic(err)
			}
			log.Println(string(output))
		}

		return nil
	})
}

func Validate(data []byte) error {
	return nil
}
