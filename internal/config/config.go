package config

import "github.com/tierklinik-dobersberg/apis/pkg/service"

type Config struct {
	service.MongoConfig
	service.BaseConfig

	Country string `env:"COUNTRY, default=AT"`
}
