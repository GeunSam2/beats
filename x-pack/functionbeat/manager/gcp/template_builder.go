// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package gcp

import (
	"time"

	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/beats/x-pack/functionbeat/function/core"
	"github.com/elastic/beats/x-pack/functionbeat/function/provider"
)

const (
	runtime          = "go111"        // Golang 1.11
	entryPoint       = "functionbeat" // entrypoint
	sourceArchiveURL = "gs://%s"      // location of functionbeat.zip
)

// NewTemplateBuilder returns the requested template builder
func NewTemplateBuilder(log *logp.Logger, cfg *common.Config, p provider.Provider) (provider.TemplateBuilder, error) {
	// TODO fncfg
	// TODO simacfg
	return newRestAPITemplateBuilder(log, cfg, p)
}

// restAPITemplateBuilder builds request object when deploying Functionbeat using
// the command deploy.
type restAPITemplateBuilder struct {
	log            *logp.Logger
	gcpConfig      Config
	functionConfig functionConfig
}

type functionData struct {
	raw         []byte
	requestBody map[string]interface{}
}

// newRestAPITemplateBuilder
func newRestAPITemplateBuilder(log *logp.Logger, cfg *common.Config, p provider.Provider) (provider.TemplateBuilder, error) {
	return &restAPITemplateBuilder{log: log}, nil
}

func (r *restAPITemplateBuilder) execute(name string) (*functionData, error) {
	r.log.Debug("Compressing all assets into an artifact")
	raw, err := core.MakeZip("gcp")
	if err != nil {
		return nil, err
	}

	r.log.Debugf("Compression is successful (zip size: %d bytes)", len(raw))

	return &functionData{
		raw:         raw,
		requestBody: r.requestBody(name),
	}, nil
}

func (r *restAPITemplateBuilder) requestBody(name string) common.MapStr {
	body := common.MapStr{
		"name":                 name,
		"description":          r.functionConfig.Description,
		"entryPoint":           entryPoint,
		"runtime":              runtime,
		"sourceUploadUrl":      r.gcpConfig.FunctionStorage,
		"eventTrigger":         r.functionConfig.Trigger,
		"environmentVariables": common.MapStr{}, // TODO pass beats variables
	}
	if r.functionConfig.Timeout > 0*time.Second {
		body["timeout"] = r.functionConfig.Timeout.String()
	}
	if r.functionConfig.MemorySize > 0 {
		body["memorySize"] = r.functionConfig.MemorySize
	}
	if len(r.functionConfig.ServiceAccountEmail) > 0 {
		body["serviceAccountEmail"] = r.functionConfig.ServiceAccountEmail
	}
	if len(r.functionConfig.Labels) > 0 {
		body["labels"] = r.functionConfig.Labels
	}
	if r.functionConfig.MaxInstances > 0 {
		body["maxInstances"] = r.functionConfig.MaxInstances
	}
	if len(r.functionConfig.VPCConnector) > 0 {
		body["vpcConnector"] = r.functionConfig.VPCConnector
	}
	return body
}

// RawTemplate returns the JSON to POST to the endpoint.
func (r *restAPITemplateBuilder) RawTemplate(name string) (string, error) {
	return r.requestBody(name).StringToPrint(), nil
}

// deploymentManaegerTemplateBuilder builds a YAML configuration for users
// to deploy the exported configuration using Google Deployment Manager.
type deploymentManaegerTemplateBuilder struct {
	functionConfig functionConfig
}

// newDeploymentManagerTemplateBuilder
func newDeploymentManagerTemplateBuilder(log *logp.Logger, cfg *common.Config, p provider.Provider) (provider.TemplateBuilder, error) {
	return &deploymentManaegerTemplateBuilder{}, nil
}

// RawTemplate returns YAML representation of the function to be deployed.
func (d *deploymentManaegerTemplateBuilder) RawTemplate(name string) (string, error) {
	return "", nil
}
