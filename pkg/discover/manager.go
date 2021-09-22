package discover

import (
	"context"
	"log"
	"strings"
	"sync"
	"time"

	"go.etcd.io/etcd/clientv3"
)

type Manager struct {
	f        func([]byte) (interface{}, error)
	watchKey string

	mtx     sync.RWMutex
	targets map[string]interface{}
	syncCh  chan map[string]interface{}

	updatert    time.Duration
	triggerSend chan struct{}
}

func NewManger(watchKey string, f func([]byte) (interface{}, error)) *Manager {
	return &Manager{
		watchKey: watchKey,
		f:        f,

		syncCh:      make(chan map[string]interface{}),
		targets:     make(map[string]interface{}),
		updatert:    5 * time.Second,
		triggerSend: make(chan struct{}, 1),
	}
}

func (m *Manager) Run(ctx context.Context, client *clientv3.Client) {
	go m.sender(ctx)
	response, err := client.Get(ctx, m.watchKey, clientv3.WithPrefix())
	if err != nil {
		log.Printf("fail get init value: %s\n", err)
		return
	}
	m.mtx.Lock()
	for _, kv := range response.Kvs {
		t, err := m.f(kv.Value)
		if err != nil {
			log.Printf("can't unmarshal: %s", err)
			continue
		}
		m.targets[strings.TrimPrefix(string(kv.Key), m.watchKey)] = t
	}
	m.mtx.Unlock()
	select {
	case m.triggerSend <- struct{}{}:
	default:
	}

	watchChan := client.Watch(ctx, m.watchKey, clientv3.WithPrefix())
	for {
		select {
		case <-ctx.Done():
			return
		case response, ok := <-watchChan:
			if !ok {
				log.Println("Discoverer channel closed")
				return
			}

			m.mtx.Lock()
			for _, event := range response.Events {
				t, err := m.f(event.Kv.Value)
				if err != nil {
					log.Printf("can't unmarshal: %s", err)
					continue
				}
				m.targets[strings.TrimPrefix(string(event.Kv.Key), m.watchKey)] = t
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
				case m.syncCh <- m.allTargets():
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

func (m *Manager) allTargets() map[string]interface{} {
	m.mtx.RLock()
	defer m.mtx.RUnlock()

	result := make(map[string]interface{}, len(m.targets))
	for key, value := range m.targets {
		result[key] = value
	}

	return result
}

func (m *Manager) SyncCh() <-chan map[string]interface{} {
	return m.syncCh
}
