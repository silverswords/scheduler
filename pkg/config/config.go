package config

import "github.com/silverswords/scheduler/pkg/task"

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
		Steps []struct {
			Name string
			Run  string
		}
	}
}

func (c *config) New() task.Task {
	return nil
}

func Unmarshal(data []byte) (Config, error) {
	return &config{}, nil
}

func Validate(data []byte) error {
	return nil
}
