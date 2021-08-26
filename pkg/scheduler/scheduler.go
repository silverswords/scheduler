package scheduler

import (
	"context"
	"log"
	"sort"
	"sync"
	"time"

	"github.com/silverswords/scheduler/pkg/config"
	"github.com/silverswords/scheduler/pkg/schedule"
	"github.com/silverswords/scheduler/pkg/task"
)

type Scheduler struct {
	mu sync.Mutex

	// once      sync.Once
	stop      <-chan struct{}
	tasks     map[string]task.Task
	schedules []schedule.Schedule

	isRunning bool

	oldConfigs    map[string]config.Config
	configs       map[string]config.Config
	triggerReload chan struct{}
	reloadCh      chan struct{}
}

func New() *Scheduler {
	return &Scheduler{
		tasks:         make(map[string]task.Task),
		stop:          make(<-chan struct{}),
		triggerReload: make(chan struct{}, 1),
		reloadCh:      make(chan struct{}),
	}
}

func (s *Scheduler) Run(configs <-chan map[string]config.Config) {
	s.isRunning = true
	go s.reloader()
	timer := s.caculateTimer()
	for {
		select {
		case <-timer.C:
			sche := s.schedules[0]
			if task, ok := s.tasks[sche.Name()]; !ok {
				log.Printf("no such task: %s\n", sche.Name())
				continue
			} else {
				task.Do(context.TODO())
			}
			sche.Step()
			timer = s.caculateTimer()
		case newConfigs := <-configs:
			s.SetConfig(newConfigs)
			select {
			case s.triggerReload <- struct{}{}:
			default:
			}
		case <-s.reloadCh:
			timer = s.caculateTimer()
			log.Println("recaculate timer, next run after", time.Until(s.schedules[0].Next()))
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
	for key, config := range s.configs {
		if oldConfig, ok := s.oldConfigs[key]; ok {
			same, err := oldConfig.IsSame(&config)
			if err != nil {
				log.Printf("isSame error: %v", err)
			}
			if same {
				continue
			}
		}
		log.Printf("create task %s", key)
		name, task := config.NewTask()
		s.tasks[name] = task
		schedule, err := config.NewSchedule()
		if err != nil {
			log.Println(err)
			continue
		}
		s.schedules = append(s.schedules, schedule)
	}

	s.mu.Unlock()
	s.reloadCh <- struct{}{}
}

func (s *Scheduler) caculateTimer() *time.Timer {
	sort.Sort(schedule.ByTime(s.schedules))

	if len(s.schedules) == 0 || s.schedules[0].Next().IsZero() {
		return time.NewTimer(100000 * time.Hour)
	} else {
		return time.NewTimer(time.Until(s.schedules[0].Next()))
	}
}
