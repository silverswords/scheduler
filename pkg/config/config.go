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
	IsSame(*Config) (bool, error)
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
