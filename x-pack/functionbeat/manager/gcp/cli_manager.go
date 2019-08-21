// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package gcp

import (
	"context"
	"fmt"

	"github.com/pkg/errors"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"

	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/beats/x-pack/functionbeat/function/provider"
	"github.com/elastic/beats/x-pack/functionbeat/manager/executor"
	fngcp "github.com/elastic/beats/x-pack/functionbeat/provider/gcp/gcp"
)

type installer interface {
	Config() *fngcp.FunctionConfig
}

// CLIManager interacts with the AWS Lambda API to deploy, update or remove a function.
// It will take care of creating the main lambda function and ask for each function type for the
// operation that need to be executed to connect the lambda to the triggers.
type CLIManager struct {
	templateBuilder *restAPITemplateBuilder
	log             *logp.Logger
	config          *Config
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
	//access, refresh, err := c.oauthToken()
	//fmt.Println(access, refresh, err)
	//return nil
	tokenSrc, err := tokenSource()
	if err != nil {
		return err
	}

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
		location := fmt.Sprintf(locationTemplate, c.config.ProjectID, c.config.Location)
		executer.Add(newOpCreateFunction(c.log, location, tokenSrc, functionData.requestBody))
	}

	// TODO wait

	if err := executer.Execute(nil); err != nil {
		if rollbackErr := executer.Rollback(nil); rollbackErr != nil {
			return errors.Wrapf(err, "could not rollback, error: %s", rollbackErr)
		}
		return err
	}
	return nil
}

func (c *CLIManager) oauthToken() (string, string, error) {
	//config := &oauth2.Config{
	//	ClientID:     "97851287300-or6lkougl6f6l6s19d0ja3v1aom0l0s8.apps.googleusercontent.com",
	//	ClientSecret: "Hr3Icu7q2aIkbOGhwl-tX9NE",
	//	RedirectURL:  "http://kvch.me/fnbeat_state",
	//	Scopes:       []string{"https://www.googleapis.com/auth/cloud-platform"},
	//	Endpoint:     google.Endpoint,
	//}

	//// Dummy authorization flow to read auth code from stdin.
	//authURL := config.AuthCodeURL("fnbeat_state")

	//fmt.Printf("Follow the link in your browser to obtain auth code: %s\n", authURL)

	//// Read the authentication code from the command line
	//var code string
	//fmt.Printf("Enter token: ")
	//fmt.Scanln(&code)

	//token, err := config.Exchange(context.TODO(), code)
	//if err != nil {
	//	return "", "", err
	//}
	//return token.AccessToken, token.RefreshToken, nil
	src, err := google.DefaultTokenSource(context.TODO(), "https://www.googleapis.com/auth/cloud-platform")
	if err != nil {
		return "", "", err
	}
	token, err := src.Token()
	if err != nil {
		return "", "", err
	}

	return token.AccessToken, token.RefreshToken, nil
}

func tokenSource() (oauth2.TokenSource, error) {
	return google.DefaultTokenSource(context.TODO(), "https://www.googleapis.com/auth/cloud-platform")
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

	return &CLIManager{
		config:          config,
		log:             logp.NewLogger("gcp"),
		templateBuilder: templateBuilder,
	}, nil
}
