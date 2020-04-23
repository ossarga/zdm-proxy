package config

import (
	"github.com/kelseyhightower/envconfig"
	"log"
)

type Config struct {
	SourceHostname string `required:"true" split_words:"true"`
	SourceUsername string `required:"true" split_words:"true"`
	SourcePassword string `required:"true" split_words:"true"`
	SourcePort     int    `required:"true" split_words:"true"`

	AstraHostname string `required:"true" split_words:"true"`
	AstraUsername string `required:"true" split_words:"true"`
	AstraPassword string `required:"true" split_words:"true"`
	AstraPort     int    `required:"true" split_words:"true"`

	MigrationComplete          bool   `required:"true" split_words:"true"`
	MigrationServiceHostname   string `required:"true" split_words:"true"`
	MigrationCommunicationPort int    `required:"true" split_words:"true"`
	ProxyServiceHostname       string `required:"true" split_words:"true"`
	ProxyCommunicationPort     int    `required:"true" split_words:"true"`
	ProxyMetricsPort           int    `required:"true" split_words:"true"`
	ProxyQueryPort             int    `split_words:"true"`

	Test  bool
	Debug bool
}

func New() *Config {
	return &Config{}
}

func (c *Config) ParseEnvVars() *Config {
	err := envconfig.Process("", c)
	if err != nil {
		log.Panicf("could not load environment variables. Error: %s", err.Error())
	}

	return c
}
