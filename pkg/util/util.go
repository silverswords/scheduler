package util

import (
	"errors"

	"github.com/spf13/viper"
)

var (
	errNoEndpoints   = errors.New("can't find endpoints in config file")
	errWrongEndpoint = errors.New("wrong endpoint")
)

// GetEndpoints gets endpoints configuration in viper
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

// GetTaskDispatchPrefix gets the key prefix of config discover
func GetConfigPrefix() (string, error) {
	prefix, ok := viper.Get("prefix.config").(string)
	if !ok {
		return "", errors.New("can't find prefix.config in config file")
	}

	return prefix, nil
}

// GetTaskDispatchPrefix gets the key prefix of worker discover
func GetWorkerDiscoverPrefix() (string, error) {
	prefix, ok := viper.Get("prefix.worker").(string)
	if !ok {
		return "", errors.New("can't find prefix.worker in config file")
	}

	return prefix, nil
}

// GetTaskDispatchPrefix gets the key prefix of task dispatch and task receive
func GetTaskDispatchPrefix() (string, error) {
	prefix, ok := viper.Get("prefix.task").(string)
	if !ok {
		return "", errors.New("can't find prefix.task in config file")
	}

	return prefix, nil
}
