package api

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/silverswords/scheduler/pkg/config"
	"github.com/silverswords/scheduler/pkg/task"
	"github.com/silverswords/scheduler/pkg/util"
)

type Client struct {
	endpoints    []string
	configPrefix string
	taskPrefix   string
	workerPrefix string
	etcdClient   *clientv3.Client
}

func NewClient(endpoints []string, configPrefix, taskPrefix, workerPrefix string) (*Client, error) {
	etcdClient, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	})

	if err != nil {
		return nil, err
	}

	return &Client{
		endpoints,
		configPrefix,
		taskPrefix,
		workerPrefix,
		etcdClient,
	}, nil
}

func (c *Client) GetOriginClient() *clientv3.Client {
	return c.etcdClient
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

func (c *Client) DeliverTask(ctx context.Context, worker string, task *task.RemoteTask) error {
	value, err := task.Encode()
	if err != nil {
		log.Println(err)
		return err
	}

	key := c.taskPrefix + worker + "/" + task.Name + time.Now().Format(time.RFC3339)
	if _, err := c.etcdClient.Put(ctx, key, string(value)); err != nil {
		log.Println("deliver task fail:", err)
		return err
	}

	return nil
}

func (c *Client) Watch(ctx context.Context, key string) clientv3.WatchChan {
	ch := c.etcdClient.Watch(ctx, key, clientv3.WithPrefix())
	return ch
}
