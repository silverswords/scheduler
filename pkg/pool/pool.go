package pool

import (
	"context"
	"fmt"
	"log"
	"sort"
	"sync"
	"time"

	"github.com/silverswords/scheduler/pkg/config"
	"github.com/silverswords/scheduler/pkg/message"
	"github.com/silverswords/scheduler/pkg/schedule"
	"github.com/silverswords/scheduler/pkg/task"
	"github.com/spf13/viper"
)

type Pool struct {
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

func New() *Pool {
	return &Pool{
		tasks:         make(map[string]task.Task),
		stop:          make(<-chan struct{}),
		triggerReload: make(chan struct{}, 1),
		reloadCh:      make(chan struct{}),
	}
}

func (p *Pool) Run(configs <-chan map[string]config.Config) {
	p.isRunning = true
	go p.reloader()
	timer := p.caculateTimer()
	for {
		select {
		case <-timer.C:
			sche := p.schedules[0]
			if task, ok := p.tasks[sche.Name()]; !ok {
				log.Printf("no such task: %s\n", sche.Name())
				continue
			} else {
				go task.Do(context.TODO())
				data := <-task.Get()
				if data.Err != nil {
					log.Println("task execute failed, err: ", data.Err)
				}

				emailStart, ok := viper.Get("pusher.email.start").(int)
				if !ok {
					fmt.Println("email push config error: please input the int type")
				}
				if emailStart == 1 {
					addrs, ok := viper.Get("pusher.email.addrs").([]interface{})
					if !ok {
						fmt.Println("email push config error: can't find addrs in config file")
					}
					addresses := make([]string, 0)
					for _, addr := range addrs {
						addr, ok := addr.(string)
						if !ok {
							fmt.Println("email push config error: wrong addr")
						}

						addresses = append(addresses, addr)
					}
					summary := fmt.Sprintf("task %s has been finished", sche.Name())
					err := message.EmailPush(addresses, summary, data.Data)
					if err != nil {
						fmt.Println("email push failed", err)
					}
				}

				wxStart, ok := viper.Get("pusher.wx.start").(int)
				if !ok {
					fmt.Println("wx push config error: please input the int type")
				}
				if wxStart == 1 {
					// wxPush
					summary := fmt.Sprintf("task %s has been finished", sche.Name())
					err := message.WxPush(summary, data.Data)
					if err != nil {
						fmt.Println("wx push failed", err)
					}
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
		case newConfigs := <-configs:
			p.SetConfig(newConfigs)
			select {
			case p.triggerReload <- struct{}{}:
			default:
				fmt.Println("default")
			}
		case <-p.reloadCh:
			timer = p.caculateTimer()
			if p.schedules[0].Kind() == "once" {
				log.Printf("recaculate timer, next task is %s, next run right now\n", p.schedules[0].Name())
				continue
			}
			log.Printf("recaculate timer, next task is %s, next run after %s\n", p.schedules[0].Name(), time.Until(p.schedules[0].Next()))
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
			same, err := oldConfig.IsSame(&config)
			if err != nil {
				log.Printf("isSame error: %v", err)
			}
			if same {
				if config.GetTag() == "new" {
					schedule, err := config.NewSchedule()
					if err != nil {
						log.Println(err)
						continue
					}
					if schedule.Kind() == "once" {
						for i, s := range p.schedules {
							if s.Name() == schedule.Name() {
								p.schedules[i] = schedule
							}
						}
					}
				}
				config.SetTag("old")
				continue
			} else {
				config.SetTag("old")
				name, task := config.NewTask()
				log.Printf("task %s has been created", name)
				p.tasks[name] = task
				schedule, err := config.NewSchedule()
				if err != nil {
					log.Println(err)
					continue
				}
				for i, s := range p.schedules {
					if s.Name() == schedule.Name() {
						p.schedules[i] = schedule
					}
				}
				continue
			}
		}
		config.SetTag("old")
		name, task := config.NewTask()
		log.Printf("task %s has been created", name)
		p.tasks[name] = task
		schedule, err := config.NewSchedule()
		if err != nil {
			log.Println(err)
			continue
		}
		p.schedules = append(p.schedules, schedule)
	}

	p.mu.Unlock()
	p.reloadCh <- struct{}{}
}

func (p *Pool) caculateTimer() *time.Timer {
	sort.Sort(schedule.ByTime(p.schedules))

	if len(p.schedules) == 0 || p.schedules[0].Next().IsZero() {
		return time.NewTimer(1000000 * time.Hour)
	} else {
		return time.NewTimer(time.Until(p.schedules[0].Next()))
	}
}
