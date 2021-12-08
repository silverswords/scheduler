package api

import (
	"context"
	"fmt"
	"os"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/silverswords/scheduler/pkg/config"
	"github.com/silverswords/scheduler/pkg/util"
)

const (
	defaultConfigPrefix = "config"
	defaultWorkerPrefix = "worker"
	defaultTaskPrefix   = "task"
)

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
	return c.configPrefix
}

func (c *Client) WorkerPrefix() string {
	return c.workerPrefix
}

func (c *Client) TaskPrefix() string {
	return c.taskPrefix
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

	prefix, err := util.GetConfigPrefix()
	if err != nil {
		return err
	}

	if _, err := c.etcdClient.Put(ctx, prefix+config.Name, string(data)); err != nil {
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

	prefix, err := util.GetConfigPrefix()
	if err != nil {
		return err
	}

	if _, err := c.etcdClient.Delete(ctx, prefix+config.Name); err != nil {
		return err
	}

	return nil
}

func (c *Client) ListAllConfig(ctx context.Context) error {
	resp, err := c.etcdClient.Get(ctx, c.configPrefix, clientv3.WithPrefix())
	if err != nil {
		return err
	}

	for k, v := range resp.Kvs {
		fmt.Println(k, v)
	}

	return nil
}

func (c *Client) ListTasks(ctx context.Context) error {
	resp, err := c.etcdClient.Get(ctx, c.taskPrefix, clientv3.WithPrefix())
	if err != nil {
		return err
	}

	for k, v := range resp.Kvs {
		fmt.Println(k, v)
	}

	return nil
}

func (c *Client) ListWorkers(ctx context.Context) error {
	resp, err := c.etcdClient.Get(ctx, c.workerPrefix, clientv3.WithPrefix())
	if err != nil {
		return err
	}

	for k, v := range resp.Kvs {
		fmt.Println(k, v)
	}

	return nil
}

func (c *Client) Watch(ctx context.Context, key string) clientv3.WatchChan {
	ch := c.etcdClient.Watch(ctx, key, clientv3.WithPrefix())
	return ch
}
