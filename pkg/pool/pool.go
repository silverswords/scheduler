package pool

import (
	"container/heap"
	"context"
	"log"
	"path"
	"sort"
	"strconv"
	"sync"
	"time"

	taskspb "github.com/silverswords/scheduler/api/tasks"
	"github.com/silverswords/scheduler/pkg/api"
	"github.com/silverswords/scheduler/pkg/config"
	"github.com/silverswords/scheduler/pkg/schedule"
	"github.com/silverswords/scheduler/pkg/worker"
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

	runningMu     sync.RWMutex
	runningConfig configHeap
	triggerReload chan struct{}
	syncCh        chan struct{}

	workers map[string]*innerWorker

	taskspb.UnimplementedStateChangeServer
}

// New creates a pool
func New() *Pool {
	queue := NewQueue()
	queue.SetCompareFunc( // CompareByPriority is the Less function used priority
		CompareFunc(func(t1, t2 *taskspb.TaskInfo) bool {
			return t1.Priority < t2.Priority
		}),
	)

	return &Pool{
		scheduleSet:   schedule.NewHeapSet(),
		queue:         queue,
		configs:       make(map[string]*config.Config),
		stop:          make(chan struct{}),
		triggerReload: make(chan struct{}, 1),
		syncCh:        make(chan struct{}),
		workers:       make(map[string]*innerWorker),
	}
}

// Run -
func (p *Pool) Run(client *api.Client, configs <-chan map[string]interface{}, workers <-chan map[string]interface{}) {
	p.isRunning = true
	go p.reloader()
	go p.dispatcher(client)
	etcdClient := client.GetOriginClient()
	SetStepStateChangeHook(func(s *step) {
		_, err := etcdClient.Put(context.TODO(), path.Join(client.TaskPrefix(), s.c.name,
			strconv.FormatInt(s.c.StartTime.UnixMicro(), 10), s.Name+"-"+strconv.Itoa(s.retryTimes)), s.state.String())
		if err != nil {
			log.Println(err)
		}
	})

	SetConfigStateChangeHook(func(c *runningConfig) {
		_, err := etcdClient.Put(context.TODO(), path.Join(client.TaskPrefix(), c.name,
			strconv.FormatInt(c.StartTime.UnixMicro(), 10)), c.state.String())
		if err != nil {
			log.Println(err)
		}
	})

	timer := p.caculateTimer()
	for {
		select {
		case <-timer.C:
			sche := p.scheduleSet.First()
			p.mu.RLock()
			running := fromConfig(p.configs[sche.Name()])
			p.mu.RUnlock()

			p.runningMu.Lock()
			heap.Push(p.runningConfig, running)
			tasks, err := running.Graph()
			p.runningMu.Unlock()

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
				newConfigs[k] = v.(*config.Config)
			}

			p.setConfig(newConfigs)
			select {
			case p.triggerReload <- struct{}{}:
			default:
			}

		case newWorkers := <-workers:
			new := make(map[string]*innerWorker)
			for k, v := range newWorkers {
				w, ok := p.workers[k]
				if !ok {
					var err error
					w, err = newWorker(v.(*worker.Config))
					if err != nil {
						continue
					}
				}
				new[k] = w
			}

			p.mu.Lock()
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

		task := p.queue.Get()
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
			if err := worker.deliver(task); err != nil {
				log.Printf("deliver task fail, worker: %s, task: %s, error: %v\n", worker.config.Name, task.Name, err)
				continue
			}
			p.queue.Done(task)
			log.Printf("deliver task success, worker: %s, task: %s\n", worker.config.Name, task.Name)
			break
		}
	}

}

func filterWokers(lables []string, workers map[string]*innerWorker) []*innerWorker {
	result := []*innerWorker{}
	for _, worker := range workers {
		flag := true
		for _, lable := range lables {
			if !worker.lables[lable] {
				flag = false
				break
			}
		}
		if flag {
			result = append(result, worker)
		}
	}

	return result
}
