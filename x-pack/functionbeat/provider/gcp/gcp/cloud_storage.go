// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package gcp

import (
	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/x-pack/functionbeat/function/provider"
)

func NewCloudStorage(provider provider.Provider, config *common.Config) (provider.Function, error) {
	return nil, nil
}
