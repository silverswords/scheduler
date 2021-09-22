package util

import (
	"errors"

	"github.com/spf13/viper"
)

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
