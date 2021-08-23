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
	syncCh  chan []config.Config

	updatert    time.Duration
	triggerSend chan struct{}
}

func NewManger() *Manager {
	return &Manager{
		syncCh:      make(chan []config.Config),
		targets:     make(map[string]config.Config),
		updatert:    5 * time.Second,
		triggerSend: make(chan struct{}, 1),
	}
}

func (m *Manager) Run(ctx context.Context) {
	go m.sender(ctx)

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:12379"},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		log.Fatalln("Can't create etcd client: ", err)
	}
	updates := client.Watch(context.TODO(), "config", clientv3.WithPrefix())

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
				m.targets[string(event.Kv.Key)] = c
			}
			m.mtx.Unlock()

			select {
			case m.triggerSend <- struct{}{}:
			default:
			}
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

func (m *Manager) allConfig() []config.Config {
	m.mtx.RLock()
	defer m.mtx.RUnlock()

	var result []config.Config
	for _, c := range m.targets {
		result = append(result, c)
	}

	return result
}

func (m *Manager) SyncCh() <-chan []config.Config {
	return m.syncCh
}
