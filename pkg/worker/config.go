package worker

import "gopkg.in/yaml.v2"

type Config struct {
	Name   string   `yaml:"name,omitempty"`
	Addr   string   `yaml:"addr,omitempty"`
	Lables []string `yaml:"lables,omitempty"`
}

func Unmarshal(data []byte) (*Config, error) {
	c := &Config{}

	if err := yaml.Unmarshal(data, c); err != nil {
		return nil, err
	}

	return c, nil
}

func Marshal(c *Config) ([]byte, error) {
	return yaml.Marshal(c)
}
