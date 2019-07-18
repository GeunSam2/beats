// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package gcp

import (
	"fmt"

	"github.com/pkg/errors"

	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/beats/x-pack/functionbeat/function/executor"
	"github.com/elastic/beats/x-pack/functionbeat/function/provider"
)

const (
	googleAPIsURL = "https://cloudfunctions.googleapis.com/v1/"
)

// CLIManager interacts with the AWS Lambda API to deploy, update or remove a function.
// It will take care of creating the main lambda function and ask for each function type for the
// operation that need to be executed to connect the lambda to the triggers.
type CLIManager struct {
	templateBuilder *restAPITemplateBuilder
	log             *logp.Logger
	config          *Config
	functionConfig  *functionConfig

	location string
}

// Deploy delegate deploy to the actual function implementation.
func (c *CLIManager) Deploy(name string) error {
	c.log.Debugf("Deploying function: %s", name)
	defer c.log.Debugf("Deploy finish for function '%s'", name)

	create := false
	err := c.deploy(create, name)
	if err != nil {
		return err
	}

	c.log.Debugf("Successfully created function '%s'", name)
	return nil
}

// Update updates the function.
func (c *CLIManager) Update(name string) error {
	c.log.Debugf("Starting updating function '%s'", name)
	defer c.log.Debugf("Update complete for function '%s'", name)

	update := true
	err := c.deploy(update, name)
	if err != nil {
		return err
	}

	c.log.Debugf("Successfully updated function: '%s'", name)
	return nil
}

// deploy uploads to bucket and creates a function on GCP.
func (c *CLIManager) deploy(update bool, name string) error {
	functionData, err := c.templateBuilder.execute(name)
	if err != nil {
		return err
	}

	executer := executor.NewExecutor(c.log)
	executer.Add(newOpEnsureBucket(c.log, c.config))
	executer.Add(newOpUploadToBucket(c.log, c.config, name, functionData.raw))

	if update {
		// TODO
	} else {
		executer.Add(newOpCreateFunction(c.log, c.location, functionData.requestBody))
	}

	// TODO wait

	ctx := newContext()
	if err := executer.Execute(ctx); err != nil {
		if rollbackErr := executer.Rollback(ctx); rollbackErr != nil {
			return errors.Wrapf(err, "could not rollback, error: %s", rollbackErr)
		}
		return err
	}
	return nil
}

// Remove removes a stack and unregister any resources created.
func (c *CLIManager) Remove(name string) error {
	c.log.Debugf("Removing function: %s", name)
	defer c.log.Debugf("Removal of function '%s' complete", name)

	// TODO

	c.log.Debugf("Successfully deleted function: '%s'", name)
	return nil
}

// NewCLI returns the interface to manage function on Amazon lambda.
func NewCLI(
	log *logp.Logger,
	cfg *common.Config,
	provider provider.Provider,
) (provider.CLIManager, error) {
	config := &Config{}
	if err := cfg.Unpack(config); err != nil {
		return nil, err
	}

	builder, err := provider.TemplateBuilder()
	if err != nil {
		return nil, err
	}

	templateBuilder, ok := builder.(*restAPITemplateBuilder)
	if !ok {
		return nil, fmt.Errorf("not restAPITemplateBuilder")
	}

	location := "projects/" + config.ProjectID + "/locations" + config.Location

	return &CLIManager{
		config:          config,
		functionConfig:  functionConfig,
		log:             logp.NewLogger("gcp"),
		templateBuilder: templateBuilder,
		location:        location,
	}, nil
}
