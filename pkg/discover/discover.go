package discover

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/silverswords/scheduler/pkg/config"
	"go.etcd.io/etcd/clientv3"
)

type Manager struct {
	mtx sync.RWMutex

	targets map[string]config.Config
	syncCh  chan map[string]config.Config

	updatert     time.Duration
	triggerSend  chan struct{}
	removeTaskCh chan []config.Config
}

func NewManger() *Manager {
	return &Manager{
		syncCh:       make(chan map[string]config.Config),
		targets:      make(map[string]config.Config),
		updatert:     5 * time.Second,
		triggerSend:  make(chan struct{}, 1),
		removeTaskCh: make(chan []config.Config),
	}
}

func (m *Manager) Run(ctx context.Context) {
	go m.sender(ctx)

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"192.168.0.251:12379"},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		log.Fatalln("Can't create etcd client: ", err)
	}
	updates := client.Watch(context.TODO(), "config update", clientv3.WithPrefix())
	removes := client.Watch(context.TODO(), "config remove", clientv3.WithPrefix())

	for {
		select {
		case <-ctx.Done():
			return
		case response, ok := <-updates:
			if !ok {
				log.Println("Discoverer channel closed")
				return
			}

			m.mtx.Lock()
			for _, event := range response.Events {
				c, err := config.Unmarshal(event.Kv.Value)
				if err != nil {
					log.Printf("Config can't be unmarshal: %s\n", err)
				}
				c.SetTag("new")
				m.targets[string(event.Kv.Key)] = c
			}
			m.mtx.Unlock()

			select {
			case m.triggerSend <- struct{}{}:
			default:
			}

		case response, ok := <-removes:
			if !ok {
				log.Println("Discoverer channel closed")
				return
			}

			if len(m.targets) == 0 {
				log.Println("please apply a config")
				continue
			}

			var configs []config.Config
			m.mtx.Lock()
			for _, event := range response.Events {
				c, err := config.Unmarshal(event.Kv.Value)
				if err != nil {
					log.Printf("Config can't be unmarshal: %s\n", err)
				}
				configs = append(configs, c)
				delete(m.targets, string(event.Kv.Key))
			}
			m.mtx.Unlock()

			m.removeTaskCh <- configs
		}
	}
}

func (m *Manager) sender(ctx context.Context) {
	ticker := time.NewTicker(m.updatert)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			select {
			case <-m.triggerSend:

				select {
				case m.syncCh <- m.allConfig():
				default:
					log.Println("Discovery receiver's channel was full so will retry the next cycle")
					select {
					case m.triggerSend <- struct{}{}:
					default:
					}
				}
			default:
			}
		}
	}
}

func (m *Manager) allConfig() map[string]config.Config {
	m.mtx.RLock()
	defer m.mtx.RUnlock()

	result := make(map[string]config.Config, len(m.targets))
	for key, value := range m.targets {
		result[key] = value
	}

	return result
}

func (m *Manager) SyncCh() <-chan map[string]config.Config {
	return m.syncCh
}

func (m *Manager) RemoveCh() <-chan []config.Config {
	return m.removeTaskCh
}
