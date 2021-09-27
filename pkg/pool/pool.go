package pool

import (
	"context"
	"fmt"
	"log"
	"sort"
	"sync"
	"time"

	"github.com/silverswords/scheduler/pkg/config"
	"github.com/silverswords/scheduler/pkg/schedule"
	"github.com/silverswords/scheduler/pkg/task"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type Pool struct {
	mu      sync.Mutex
	rwMutex sync.RWMutex

	queue Queue

	// once      sync.Once
	stop      <-chan struct{}
	schedules []schedule.Schedule

	isRunning bool

	oldConfigs    map[string]config.Config
	configs       map[string]config.Config
	triggerReload chan struct{}
	syncCh        chan struct{}

	workers map[string]bool
}

func New() *Pool {
	return &Pool{
		queue:         NewQueue(),
		stop:          make(<-chan struct{}),
		triggerReload: make(chan struct{}, 1),
		syncCh:        make(chan struct{}),
		workers:       make(map[string]bool),
	}
}

func (p *Pool) Run(client *clientv3.Client, configs <-chan map[string]interface{}, workers <-chan map[string]interface{}) {
	p.isRunning = true
	go p.reload()
	go p.reloader(configs)
	go p.dispatcher(client)

	timer := p.caculateTimer()
	for {
		select {
		case <-timer.C:
			sche := p.schedules[0]
			fmt.Println(p.workers)
			p.queue.Add(&task.RemoteTask{
				Name:      sche.Name(),
				StartTime: sche.Next(),
			})
			sche.Step()
			timer = p.caculateTimer()
			p.mu.Lock()
			nextSche := p.schedules[0]

			if time.Until(nextSche.Next()).Hours() < -10000 {
				log.Println("all task have been completed")
				continue
			}

			if nextSche.Kind() == "once" {
				log.Printf("recaculate timer, next task is %s, next run right now\n", nextSche.Name())
				continue
			}

			log.Printf("recaculate timer, next task is %s, next run after %s\n", nextSche.Name(), time.Until(nextSche.Next()))
			p.mu.Unlock()

		case <-p.syncCh:
			timer = p.caculateTimer()
			p.mu.Lock()
			if len(p.schedules) == 0 {
				continue
			}
			sche := p.schedules[0]
			p.mu.Unlock()

			if sche.Kind() == "once" {
				log.Printf("recaculate timer, next task is %s, next run right now\n%v\n", sche.Name(), timer)
				continue
			}

			log.Printf("recaculate timer, next task is %s, next run after %s\n", sche.Name(), time.Until(sche.Next()))

		case newWorkers := <-workers:
			p.rwMutex.Lock()
			for k, v := range newWorkers {
				if v != "online" {
					p.workers[k] = false
				} else {
					p.workers[k] = true
				}
			}

			log.Printf("update worker list: %v", p.workers)
			p.rwMutex.Unlock()

		case <-p.stop:
			return
		}
	}
}

func (p *Pool) reloader(configs <-chan map[string]interface{}) {
	for {
		select {
		case new := <-configs:
			newConfigs := make(map[string]config.Config)
			for k, v := range new {
				newConfigs[k] = v.(config.Config)
			}

			p.SetConfig(newConfigs)
			select {
			case p.triggerReload <- struct{}{}:
			default:
			}

		case <-p.stop:
			return
		}
	}
}

func (p *Pool) SetConfig(configs map[string]config.Config) {
	p.mu.Lock()
	p.oldConfigs, p.configs = p.configs, configs
	p.mu.Unlock()
}

func (p *Pool) reload() {
	ticker := time.NewTicker(5 * time.Second)

	for {
		select {
		case <-ticker.C:
			select {
			case <-p.triggerReload:
				p.sync()
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

func (p *Pool) sync() {
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

			log.Printf("%s config update: \n	old: %v\n	new: %v\n", key, oldConfig, config)
		}

		schedule, err := config.NewSchedule()
		if err != nil {
			log.Println(err)
			continue
		}
		p.schedules = append(p.schedules, schedule)
	}

	p.mu.Unlock()
	p.syncCh <- struct{}{}
}

func (p *Pool) caculateTimer() *time.Timer {
	sort.Sort(schedule.ByTime(p.schedules))

	if len(p.schedules) == 0 || p.schedules[0].Next().IsZero() {
		return time.NewTimer(1000000 * time.Hour)
	} else {
		return time.NewTimer(time.Until(p.schedules[0].Next()))
	}
}

func (p *Pool) dispatcher(client *clientv3.Client) {
	for {
		task := p.queue.Get().(*task.RemoteTask)
		value, err := task.Encode()
		if err != nil {
			log.Println(err)
			continue
		}

		if len(p.workers) == 0 {
			log.Println("task scheduling failed, no worker is running")
			p.queue.Done(task)
			p.queue.Add(task)

			for len(p.workers) == 0 {
			}
			continue
		}

		for k, v := range p.workers {
			if v {
				key := "worker/" + k + "/" + task.Name + time.Now().Format(time.RFC3339)
				if _, err := client.Put(context.Background(), key, string(value)); err != nil {
					log.Println("deliver task fail:", err)
					continue
				}
				log.Printf("deliver task success, worker: %s, task: %s", k, task.Name)
				go func() {
					watchChan := client.Watch(context.Background(), key)
				watch:
					events, ok := <-watchChan
					if !ok {
						log.Printf("watch task %s channel has closed", task.Name)
						return
					}

					for _, event := range events.Events {
						err := task.Decode(event.Kv.Value)
						if err != nil {
							log.Printf("can't unmarshal task, err:%s\n", err)
							continue
						}

						if !task.Done {
							goto watch
						}
						p.queue.Done(task)
						if task.Err != nil {
							log.Printf("task run fail, error: %s\n", task.Err)
							task.Done, task.Err = false, nil
							p.queue.Add(task)
						}

						log.Printf("task %s run success\n", task.Name)
					}
				}()
				break
			}
		}
	}

}
