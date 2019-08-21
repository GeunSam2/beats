// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package gcp

import (
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/beats/x-pack/functionbeat/function/provider"
	"github.com/elastic/beats/x-pack/functionbeat/manager/core"
	"github.com/elastic/beats/x-pack/functionbeat/manager/core/bundle"
	fngcp "github.com/elastic/beats/x-pack/functionbeat/provider/gcp/gcp"
)

const (
	runtime          = "go111"                            // Golang 1.11
	sourceArchiveURL = "gs://%s/%s"                       // path to the function archive
	locationTemplate = "projects/%s/locations/%s"         // full name of the location
	functionName     = locationTemplate + "/functions/%s" // full name of the functions
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
	provider  provider.Provider
	log       *logp.Logger
	gcpConfig *Config
}

type functionData struct {
	raw         []byte
	requestBody common.MapStr
}

// newRestAPITemplateBuilder
func newRestAPITemplateBuilder(log *logp.Logger, cfg *common.Config, p provider.Provider) (provider.TemplateBuilder, error) {
	gcpCfg := &Config{}
	err := cfg.Unpack(gcpCfg)
	if err != nil {
		return &restAPITemplateBuilder{}, err
	}

	return &restAPITemplateBuilder{log: log, gcpConfig: gcpCfg, provider: p}, nil
}

func (r *restAPITemplateBuilder) execute(name string) (*functionData, error) {
	r.log.Debug("Compressing all assets into an artifact")

	raw, err := core.MakeZip(ZipResources())
	if err != nil {
		return nil, err
	}

	r.log.Debugf("Compression is successful (zip size: %d bytes)", len(raw))

	fn, err := findFunction(r.provider, name)
	if err != nil {
		return nil, err
	}

	return &functionData{
		raw:         raw,
		requestBody: r.requestBody(name, fn.Config()),
	}, nil
}

func findFunction(p provider.Provider, name string) (installer, error) {
	fn, err := p.FindFunctionByName(name)
	if err != nil {
		return nil, err
	}

	function, ok := fn.(installer)
	if !ok {
		return nil, errors.New("incompatible type received, expecting: 'functionManager'")
	}

	return function, nil
}

func (r *restAPITemplateBuilder) requestBody(name string, config *fngcp.FunctionConfig) common.MapStr {
	fnName := fmt.Sprintf(functionName, r.gcpConfig.ProjectID, r.gcpConfig.Location, name)
	body := common.MapStr{
		"name":             fnName,
		"description":      config.Description,
		"entryPoint":       config.EntryPoint(),
		"runtime":          runtime,
		"sourceArchiveUrl": fmt.Sprintf(sourceArchiveURL, r.gcpConfig.FunctionStorage, name),
		"eventTrigger":     config.Trigger,
		"environmentVariables": common.MapStr{
			"ENABLED_FUNCTIONS": name,
		},
	}
	if config.Timeout > 0*time.Second {
		body["timeout"] = config.Timeout.String()
	}
	if config.MemorySize > 0 {
		body["memorySize"] = config.MemorySize
	}
	if len(config.ServiceAccountEmail) > 0 {
		body["serviceAccountEmail"] = config.ServiceAccountEmail
	}
	if len(config.Labels) > 0 {
		body["labels"] = config.Labels
	}
	if config.MaxInstances > 0 {
		body["maxInstances"] = config.MaxInstances
	}
	if len(config.VPCConnector) > 0 {
		body["vpcConnector"] = config.VPCConnector
	}
	return body
}

// RawTemplate returns the JSON to POST to the endpoint.
func (r *restAPITemplateBuilder) RawTemplate(name string) (string, error) {
	fn, err := findFunction(r.provider, name)
	if err != nil {
		return "", err
	}
	return r.requestBody(name, fn.Config()).StringToPrint(), nil
}

// deploymentManaegerTemplateBuilder builds a YAML configuration for users
// to deploy the exported configuration using Google Deployment Manager.
type deploymentManaegerTemplateBuilder struct {
}

// newDeploymentManagerTemplateBuilder
func newDeploymentManagerTemplateBuilder(log *logp.Logger, cfg *common.Config, p provider.Provider) (provider.TemplateBuilder, error) {
	return &deploymentManaegerTemplateBuilder{}, nil
}

// RawTemplate returns YAML representation of the function to be deployed.
func (d *deploymentManaegerTemplateBuilder) RawTemplate(name string) (string, error) {
	return "", nil
}

// ZipResources return the list of zip resources
func ZipResources() []bundle.Resource {
	// TODO
	f := "pubsub"
	return []bundle.Resource{
		&bundle.LocalFile{Path: filepath.Join("pkg", f, f+".go"), FileMode: 0755},
		&bundle.LocalFile{Path: filepath.Join("pkg", f, "go.mod"), FileMode: 0655},
		&bundle.LocalFile{Path: filepath.Join("pkg", f, "go.sum"), FileMode: 0655},
	}
}
