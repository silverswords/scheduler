package util

import (
	"errors"
	"time"

	"github.com/spf13/viper"
	clientv3 "go.etcd.io/etcd/client/v3"
)

var globalEtcdClient *clientv3.Client

var (
	errNoEndpoints   = errors.New("can't find endpoints in config file")
	errWrongEndpoint = errors.New("wrong endpoint")
)

func GetEndpoints() ([]string, error) {
	endpoints, ok := viper.Get("endpoints").([]interface{})
	if !ok {
		return nil, errNoEndpoints
	}

	eps := make([]string, 0)
	for _, endpoint := range endpoints {
		endpoint, ok := endpoint.(string)
		if !ok {
			return nil, errWrongEndpoint
		}

		eps = append(eps, endpoint)
	}

	return eps, nil
}

func GetEtcdClient() (*clientv3.Client, error) {
	if globalEtcdClient != nil {
		return globalEtcdClient, nil
	}

	endpoints, err := GetEndpoints()
	if err != nil {
		return nil, err
	}

	globalEtcdClient, err = clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, err
	}

	return globalEtcdClient, nil
}
