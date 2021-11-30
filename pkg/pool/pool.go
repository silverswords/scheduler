package pool

import (
	"context"
	"log"
	"sort"
	"sync"
	"time"

	taskspb "github.com/silverswords/scheduler/api/tasks"
	"github.com/silverswords/scheduler/pkg/api"
	"github.com/silverswords/scheduler/pkg/config"
	"github.com/silverswords/scheduler/pkg/schedule"
	"github.com/silverswords/scheduler/pkg/task"
)

// Pool is the core of the scheduler
type Pool struct {
	mu sync.RWMutex

	queue Queue

	// once      sync.Once
	stop        chan struct{}
	scheduleSet *schedule.HeapSet

	isRunning bool

	oldConfigs map[string]*config.Config
	configs    map[string]*config.Config

	runningConfig []*runningConfig
	triggerReload chan struct{}
	syncCh        chan struct{}

	workers map[string]map[string]bool

	taskspb.UnimplementedTasksServer
}

// New creates a pool
func New() *Pool {
	queue := NewQueue()
	queue.SetCompareFunc( // CompareByPriority is the Less function used priority
		CompareFunc(func(t1, t2 task.Task) bool {
			return t1.(*task.RemoteTask).Priority < t2.(*task.RemoteTask).Priority
		}),
	)

	return &Pool{
		scheduleSet:   schedule.NewHeapSet(),
		queue:         queue,
		stop:          make(chan struct{}),
		triggerReload: make(chan struct{}, 1),
		syncCh:        make(chan struct{}),
		workers:       make(map[string]map[string]bool),
	}
}

// Run -
func (p *Pool) Run(client *api.Client, configs <-chan map[string]*config.Config, workers <-chan map[string]interface{}) {
	p.isRunning = true
	go p.reloader()
	go p.dispatcher(client)

	timer := p.caculateTimer()
	for {
		select {
		case <-timer.C:
			sche := p.scheduleSet.First()
			running := fromConfig(p.configs[sche.Name()])
			p.runningConfig = append(p.runningConfig, running)

			tasks, err := running.Graph()
			if err != nil {
				log.Printf("config error: %s\n", err)
				continue
			}

			for _, t := range tasks {
				p.queue.Add(t)
			}

			sche.Step()
			p.mu.Lock()
			timer = p.caculateTimer()
			nextSche := p.scheduleSet.First()

			if time.Until(nextSche.Next()).Hours() < -10000 {
				log.Println("all task have been completed")
			} else if nextSche.Kind() == "once" {
				log.Printf("recaculate timer, next task is %s, next run right now\n", nextSche.Name())
			} else {
				log.Printf("recaculate timer, next task is %s, next run after %s\n", nextSche.Name(), time.Until(nextSche.Next()))
			}

			p.mu.Unlock()

		case <-p.syncCh:
			p.mu.Lock()
			timer = p.caculateTimer()
			if p.scheduleSet.Len() == 0 {
				p.mu.Unlock()
				continue
			}

			sche := p.scheduleSet.First()
			p.mu.Unlock()

			if sche.Kind() == "once" {
				log.Printf("recaculate timer, next task is %s, next run right now\n%v\n", sche.Name(), timer)
			} else {
				log.Printf("recaculate timer, next task is %s, next run after %s\n", sche.Name(), time.Until(sche.Next()))
			}

		case new := <-configs:
			newConfigs := make(map[string]*config.Config)
			for k, v := range new {
				newConfigs[k] = v
			}

			p.setConfig(newConfigs)
			select {
			case p.triggerReload <- struct{}{}:
			default:
			}

		case newWorkers := <-workers:
			p.mu.Lock()
			new := make(map[string]map[string]bool)
			for k, v := range newWorkers {
				new[k] = make(map[string]bool)
				for _, lable := range v.([]string) {
					new[k][lable] = true
				}
			}

			p.workers = new
			log.Printf("update worker list: %v", p.workers)
			p.mu.Unlock()

		case <-p.stop:
			return
		}
	}
}

func (p *Pool) setConfig(configs map[string]*config.Config) {
	p.mu.Lock()
	p.oldConfigs, p.configs = p.configs, configs
	p.mu.Unlock()
}

func (p *Pool) reloader() {
	ticker := time.NewTicker(5 * time.Second)

	for {
		select {
		case <-ticker.C:
			select {
			case <-p.triggerReload:
				p.reload()
			case <-p.stop:
				ticker.Stop()
				return
			}

		case <-p.stop:
			ticker.Stop()
			return
		}
	}
}

func (p *Pool) reload() {
	p.mu.Lock()
	for key, config := range p.configs {
		if oldConfig, ok := p.oldConfigs[key]; ok {
			same, err := oldConfig.IsSame(config)
			if err != nil {
				log.Printf("isSame error: %v", err)
				continue
			}

			if same {
				continue
			}

			log.Printf("%s config update:\nold: %v\nnew: %v\n", key, oldConfig, config)
		}

		schedule, err := config.NewSchedule()
		if err != nil {
			log.Println(err)
			continue
		}

		p.scheduleSet.Add(schedule)
	}

	p.mu.Unlock()
	p.syncCh <- struct{}{}
}

func (p *Pool) caculateTimer() *time.Timer {
	sort.Sort(p.scheduleSet)

	if p.scheduleSet.Len() == 0 || p.scheduleSet.First().Next().IsZero() {
		return time.NewTimer(1000000 * time.Hour)
	} else {
		return time.NewTimer(time.Until(p.scheduleSet.First().Next()))
	}
}

func (p *Pool) Stop() {
	close(p.stop)
}

func (p *Pool) dispatcher(client *api.Client) {
	for {
		for len(p.workers) == 0 {
			time.Sleep(5 * time.Second)
			log.Println("block for empty workers")
		}

		task := p.queue.Get().(*task.RemoteTask)
		p.mu.RLock()
		workers := filterWokers(task.Lables, p.workers)
		p.mu.RUnlock()
		if len(workers) == 0 {
			log.Println("task scheduling failed, no worker who meet the labels is running")
			p.queue.Done(task)
			p.queue.Add(task)
			continue
		}

		for _, worker := range workers {
			err := client.DeliverTask(context.Background(), worker, task)
			if err != nil {
				log.Println("deliver task failed, error: ", err)
				continue
			}

			log.Printf("deliver task success, worker: %s, task: %s", worker, task.Name)
			break
		}
	}

}

func filterWokers(lables []string, worker map[string]map[string]bool) []string {
	result := []string{}
	for k, v := range worker {
		flag := true
		for _, lable := range lables {
			if !v[lable] {
				flag = false
				break
			}
		}
		if flag {
			result = append(result, k)
		}
	}

	return result
}
