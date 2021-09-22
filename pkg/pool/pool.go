package pool

import (
	"fmt"
	"log"
	"sort"
	"sync"
	"time"

	"github.com/coreos/etcd/Godeps/_workspace/src/golang.org/x/net/context"
	"github.com/silverswords/scheduler/pkg/config"
	"github.com/silverswords/scheduler/pkg/schedule"
	"go.etcd.io/etcd/clientv3"
)

type Pool struct {
	mu sync.Mutex

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

	timer := p.caculateTimer()

	for {
		select {
		case <-timer.C:
			sche := p.schedules[0]
			fmt.Println(p.workers)

			for k, v := range p.workers {
				if v {
					if _, err := client.Put(context.Background(), "worker/"+k+"/"+sche.Name()+time.Now().Format(time.RFC3339), sche.Name()); err != nil {
						log.Println("deliver task fail:", err)
						continue
					}
					log.Printf("deliver task success, worker: %s, task: %s", k, sche.Name())
					break
				}
			}

			sche.Step()

			timer = p.caculateTimer()
			if time.Until(p.schedules[0].Next()).Hours() < -10000 {
				log.Println("all task have been completed")
				continue
			}

			if p.schedules[0].Kind() == "once" {
				log.Printf("recaculate timer, next task is %s, next run right now\n", p.schedules[0].Name())
				continue
			}

			log.Printf("recaculate timer, next task is %s, next run after %s\n", p.schedules[0].Name(), time.Until(p.schedules[0].Next()))

		case <-p.syncCh:
			timer = p.caculateTimer()
			if p.schedules[0].Kind() == "once" {
				log.Printf("recaculate timer, next task is %s, next run right now\n%v\n", p.schedules[0].Name(), timer)
				continue
			}

			log.Printf("recaculate timer, next task is %s, next run after %s\n", p.schedules[0].Name(), time.Until(p.schedules[0].Next()))
		case newWorkers := <-workers:
			for k, v := range newWorkers {
				if v != "online" {
					p.workers[k] = false
				}
				p.workers[k] = true
			}

			log.Printf("update worker list: %v", p.workers)
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
