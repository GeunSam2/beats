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

// defaultTemplateBuilder builds request object when deploying Functionbeat using
// the command deploy.
type defaultTemplateBuilder struct {
	provider  provider.Provider
	log       *logp.Logger
	gcpConfig *Config
}

type functionData struct {
	raw         []byte
	requestBody common.MapStr
}

// NewTemplateBuilder returns the requested template builder
func NewTemplateBuilder(log *logp.Logger, cfg *common.Config, p provider.Provider) (provider.TemplateBuilder, error) {
	gcpCfg := &Config{}
	err := cfg.Unpack(gcpCfg)
	if err != nil {
		return &defaultTemplateBuilder{}, err
	}

	return &defaultTemplateBuilder{log: log, gcpConfig: gcpCfg, provider: p}, nil
}

func (d *defaultTemplateBuilder) execute(name string) (*functionData, error) {
	d.log.Debug("Compressing all assets into an artifact")

	fn, err := findFunction(d.provider, name)
	if err != nil {
		return nil, err
	}

	resources := zipResources(fn.Name())
	raw, err := core.MakeZip(resources)
	if err != nil {
		return nil, err
	}

	d.log.Debugf("Compression is successful (zip size: %d bytes)", len(raw))

	return &functionData{
		raw:         raw,
		requestBody: d.requestBody(name, fn.Config()),
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

func (d *defaultTemplateBuilder) requestBody(name string, config *fngcp.FunctionConfig) common.MapStr {
	fnName := fmt.Sprintf(functionName, d.gcpConfig.ProjectID, d.gcpConfig.Location, name)
	body := common.MapStr{
		"name":             fnName,
		"description":      config.Description,
		"entryPoint":       config.EntryPoint(),
		"runtime":          runtime,
		"sourceArchiveUrl": fmt.Sprintf(sourceArchiveURL, d.gcpConfig.FunctionStorage, name),
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
func (d *defaultTemplateBuilder) RawTemplate(name string) (string, error) {
	// TODO output in YAML
	fn, err := findFunction(d.provider, name)
	if err != nil {
		return "", err
	}
	return d.requestBody(name, fn.Config()).StringToPrint(), nil
}

// ZipResources returns the list of zip resources
func ZipResources() []bundle.Resource {
	functions, err := provider.ListFunctions("gcp")
	if err != nil {
		return nil
	}

	resources := make([]bundle.Resource, 0)
	for _, f := range functions {
		resources = append(resources, zipResources(f)...)
	}
	return resources
}

func zipResources(typeName string) []bundle.Resource {
	return []bundle.Resource{
		&bundle.LocalFile{Path: filepath.Join("pkg", typeName, typeName+".go"), FileMode: 0755},
		&bundle.LocalFile{Path: filepath.Join("pkg", typeName, "go.mod"), FileMode: 0655},
		&bundle.LocalFile{Path: filepath.Join("pkg", typeName, "go.sum"), FileMode: 0655},
	}
}
