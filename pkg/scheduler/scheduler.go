package scheduler

import (
	"log"
	"sync"
	"time"

	"github.com/silverswords/scheduler/pkg/config"
	"github.com/silverswords/scheduler/pkg/task"
)

type Scheduler struct {
	mu sync.Mutex

	// once      sync.Once
	stop  <-chan struct{}
	tasks map[string]task.Task

	isRunning bool

	oldConfigs    map[string]config.Config
	configs       map[string]config.Config
	triggerReload chan struct{}
	reloadCh      chan struct{}
}

func New() *Scheduler {
	return &Scheduler{
		stop:          make(<-chan struct{}),
		triggerReload: make(chan struct{}, 1),
		reloadCh:      make(chan struct{}),
	}
}
func (s *Scheduler) Run(configs <-chan map[string]config.Config) {
	s.isRunning = true
	go s.reloader()

	for {
		select {
		case newConfigs := <-configs:
			s.SetConfig(newConfigs)
			select {
			case s.triggerReload <- struct{}{}:
			default:
			}
		case <-s.reloadCh:
			continue
		case <-s.stop:
			return
		}

	}
}

func (s *Scheduler) SetConfig(configs map[string]config.Config) {
	s.mu.Lock()
	s.oldConfigs, s.configs = s.configs, configs
	s.mu.Unlock()
}

func (s *Scheduler) reloader() {
	ticker := time.NewTicker(5 * time.Second)
	select {
	case <-ticker.C:
		select {
		case <-s.triggerReload:
			s.reload()
		case <-s.stop:
			ticker.Stop()
			return
		}

	case <-s.stop:
		ticker.Stop()
		return
	}
}

func (s *Scheduler) reload() {
	s.mu.Lock()
	tasks := make(map[string]task.Task)
	for key, config := range s.configs {
		if oldConfig, ok := s.oldConfigs[key]; ok {
			if oldConfig == config {
				continue
			}
		}
		log.Printf("create task %s", key)
		tasks[key] = config.New()
	}

	s.tasks = tasks
	s.mu.Unlock()
	s.reloadCh <- struct{}{}
}
