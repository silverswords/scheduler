package api

import (
	"context"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/silverswords/scheduler/pkg/config"
	"github.com/silverswords/scheduler/pkg/util"
	"github.com/silverswords/scheduler/pkg/worker"
)

const (
	defaultConfigPrefix = "config"
	defaultWorkerPrefix = "worker"
	defaultTaskPrefix   = "task"
)

const defaultSep = "/"

type Client struct {
	endpoints    []string
	configPrefix string
	taskPrefix   string
	workerPrefix string
	etcdClient   *clientv3.Client
}

type Option func(*Client) error

func WithConfigPrefix(configPrefix string) Option {
	return Option(func(c *Client) error {
		c.configPrefix = configPrefix
		return nil
	})
}

func WithWorkerPrefix(workerPrefix string) Option {
	return Option(func(c *Client) error {
		c.workerPrefix = workerPrefix
		return nil
	})
}

func WithTaskPrefix(taskPrefix string) Option {
	return Option(func(c *Client) error {
		c.taskPrefix = taskPrefix
		return nil
	})
}

func WithEtcdClient(client *clientv3.Client) Option {
	return Option(func(c *Client) error {
		c.etcdClient = client
		return nil
	})
}

func NewClient(endpoints []string, ops ...Option) (*Client, error) {
	etcdClient, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, err
	}

	configPrefix, err := util.GetConfigPrefix()
	if err != nil {
		configPrefix = defaultConfigPrefix
	}

	workerPrefix, err := util.GetWorkerDiscoverPrefix()
	if err != nil {
		workerPrefix = defaultWorkerPrefix
	}

	taskPrefix, err := util.GetTaskDispatchPrefix()
	if err != nil {
		taskPrefix = defaultTaskPrefix
	}

	c := &Client{
		endpoints,
		configPrefix,
		taskPrefix,
		workerPrefix,
		etcdClient,
	}

	for _, option := range ops {
		if err := option(c); err != nil {
			return nil, err
		}
	}

	return c, nil
}

func (c *Client) GetOriginClient() *clientv3.Client {
	return c.etcdClient
}

func (c *Client) ConfigPrefix() string {
	return c.configPrefix + defaultSep
}

func (c *Client) WorkerPrefix() string {
	return c.workerPrefix + defaultSep
}

func (c *Client) TaskPrefix() string {
	return c.taskPrefix + defaultSep
}

func (c *Client) ApplyConfig(ctx context.Context, filePath string) error {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return err
	}

	config, err := config.Unmarshal(data)
	if err != nil {
		return err
	}

	if _, err := c.etcdClient.Put(ctx, path.Join(c.configPrefix, config.Name), string(data)); err != nil {
		return err
	}

	return nil
}

func (c *Client) RemoveConfig(ctx context.Context, filePath string) error {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return err
	}

	config, err := config.Unmarshal(data)
	if err != nil {
		return err
	}

	if err := c.RemoveConfigWithName(ctx, config.Name); err != nil {
		return err
	}

	return nil
}

func (c *Client) RemoveConfigWithName(ctx context.Context, name string) error {
	if _, err := c.etcdClient.Delete(ctx, path.Join(c.configPrefix, name)); err != nil {
		return err
	}

	return nil
}

func (c *Client) ListAllConfig(ctx context.Context) ([]*config.Config, error) {
	resp, err := c.etcdClient.Get(ctx, c.ConfigPrefix(), clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	result := []*config.Config{}
	for _, kv := range resp.Kvs {
		c, err := config.Unmarshal(kv.Value)
		if err != nil {
			return nil, err
		}
		result = append(result, c)
	}

	return result, nil
}

type TaskState struct {
	Name      string
	StartTime time.Time
	Steps     []*StepState
	State     string
}

type StepState struct {
	Name  string
	State string
}

func (c *Client) ListTasks(ctx context.Context) ([]*TaskState, error) {
	resp, err := c.etcdClient.Get(ctx, c.TaskPrefix(), clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	tasks := make(map[string]map[string]string)
	steps := make(map[string]map[string][]*StepState)
	for _, kv := range resp.Kvs {
		slices := strings.Split(string(kv.Key), defaultSep)
		if len(slices) == 4 {
			if _, ok := steps[slices[1]]; !ok {
				steps[slices[1]] = make(map[string][]*StepState)
			}

			steps[slices[1]][slices[2]] = append(steps[slices[1]][slices[2]], &StepState{
				Name:  slices[3],
				State: string(kv.Value),
			})
		} else if len(slices) == 3 {
			if _, ok := tasks[slices[1]]; !ok {
				tasks[slices[1]] = make(map[string]string)
			}

			tasks[slices[1]][slices[2]] = string(kv.Value)
		}
	}

	result := []*TaskState{}
	for name, temp := range tasks {
		for t, state := range temp {
			timestamp, err := strconv.ParseInt(t, 10, 64)
			if err != nil {
				return nil, err
			}

			result = append(result, &TaskState{
				Name:      name,
				StartTime: time.UnixMicro(timestamp),
				Steps:     steps[name][t],
				State:     state,
			})
		}
	}

	return result, nil
}

func (c *Client) ListWorkers(ctx context.Context) ([]*worker.Config, error) {
	resp, err := c.etcdClient.Get(ctx, c.WorkerPrefix(), clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	result := []*worker.Config{}
	for _, kv := range resp.Kvs {
		w, err := worker.Unmarshal(kv.Value)
		if err != nil {
			return nil, err
		}
		result = append(result, w)
	}

	return result, nil
}

func (c *Client) Watch(ctx context.Context, key string) clientv3.WatchChan {
	ch := c.etcdClient.Watch(ctx, key, clientv3.WithPrefix())
	return ch
}
