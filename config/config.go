// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package config

import (
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
)

const (
	cfgFile           = ".iceberg-go.yaml"
	defaultMaxWorkers = 5
)

type Config struct {
	DefaultCatalog string                   `yaml:"default-catalog"`
	Catalogs       map[string]CatalogConfig `yaml:"catalog"`
	MaxWorkers     int                      `yaml:"max-workers"`
}

type CatalogConfig struct {
	CatalogType string `yaml:"type"`
	URI         string `yaml:"uri"`
	Output      string `yaml:"output"`
	Credential  string `yaml:"credential"`
	Warehouse   string `yaml:"warehouse"`
}

func LoadConfig(configPath string) []byte {
	var path string
	if len(configPath) > 0 {
		path = configPath
	} else {
		homeDir, err := os.UserHomeDir()
		if err != nil {
			return nil
		}
		path = filepath.Join(homeDir, cfgFile)
	}
	file, err := os.ReadFile(path)
	if err != nil {
		return nil
	}

	return file
}

func ParseConfig(file []byte, catalogName string) *CatalogConfig {
	var config Config
	err := yaml.Unmarshal(file, &config)
	if err != nil {
		return nil
	}
	res, ok := config.Catalogs[catalogName]
	if !ok {
		return nil
	}

	return &res
}

func fromConfigFiles() Config {
	dir := os.Getenv("GOICEBERG_HOME")
	if dir != "" {
		dir = filepath.Join(dir, cfgFile)
	}

	var cfg Config
	if err := yaml.Unmarshal(LoadConfig(dir), &cfg); err != nil {
		return cfg
	}

	if cfg.DefaultCatalog == "" {
		cfg.DefaultCatalog = "default"
	}
	if cfg.MaxWorkers <= 0 {
		cfg.MaxWorkers = defaultMaxWorkers
	}

	return cfg
}

var EnvConfig = fromConfigFiles()
