package discover

import (
	"context"
	"errors"

	"log"
	"strings"
	"sync"
	"time"

	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// Manager is the service discovery controller, which monitors
// the key of etcd and notifies the update
type Manager struct {
	f        func([]byte) (interface{}, error)
	watchKey string

	mtx     sync.RWMutex
	targets map[string]interface{}
	syncCh  chan map[string]interface{}

	updatert    time.Duration
	triggerSend chan struct{}
}

// NewManger Create a manager that watches to watchKey
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

// Run lets the manager start watching
func (m *Manager) Run(ctx context.Context, client *clientv3.Client) error {
	go m.sender(ctx)
	response, err := client.Get(ctx, m.watchKey, clientv3.WithPrefix())
	if err != nil {
		log.Printf("fail get init value: %s\n", err)
		return err
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
			return nil

		case response, ok := <-watchChan:
			if !ok {
				return errors.New(m.watchKey + "discoverer channel closed")
			}

			m.mtx.Lock()
			for _, event := range response.Events {
				remainKey := strings.TrimPrefix(string(event.Kv.Key), m.watchKey)
				if event.Type == mvccpb.DELETE {
					delete(m.targets, remainKey)
					continue
				}
				t, err := m.f(event.Kv.Value)
				if err != nil {
					log.Printf("can't unmarshal: %s", err)
					continue
				}

				m.targets[remainKey] = t
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
					log.Println(m.watchKey + " discovery receiver's channel was full so will retry the next cycle")
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

// SyncCh is the channel for synchronization
func (m *Manager) SyncCh() <-chan map[string]interface{} {
	return m.syncCh
}
