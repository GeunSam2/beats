// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package gcp

import (
	"time"

	"github.com/elastic/beats/x-pack/functionbeat/config"
)

// FunctionConfig stores the configuration of a Google Cloud Function
type FunctionConfig struct {
	Description         string                 `config:"description"`
	MemorySize          config.MemSizeFactor64 `config:"memory_size"`
	Timeout             time.Duration          `config:"timeout" validate:"nonzero,positive"`
	ServiceAccountEmail string                 `config:"service_account_email"`
	Labels              map[string]string      `config:"labels"`
	VPCConnector        string                 `config:"vpc_connector"`
	MaxInstances        int                    `config:"maximum_instances"`
	Trigger             struct {
		EventType string `config:"event_type" validate:"required"`
		Resource  string `config:"resource" validate:"required"`
		Service   string `config:"service"`
	} `config:"trigger" validate:"required"`

	entryPoint string
}

// TODO
func defaultFunctionConfig() *FunctionConfig {
	return &FunctionConfig{}
}

// TODO
func (c *FunctionConfig) Validate() error {
	return nil
}

func (c *FunctionConfig) EntryPoint() string {
	return c.entryPoint
}
