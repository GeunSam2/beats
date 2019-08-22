// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package gcp

import (
	"context"
	"fmt"
	"io/ioutil"
	"strings"

	"golang.org/x/oauth2"

	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/beats/x-pack/functionbeat/manager/executor"
)

const (
	googleAPIsURL = "https://cloudfunctions.googleapis.com/v1/"
)

type opCreateFunction struct {
	log         *logp.Logger
	location    string
	tokenSrc    oauth2.TokenSource
	requestBody common.MapStr
}

func newOpCreateFunction(
	log *logp.Logger,
	location string,
	tokenSrc oauth2.TokenSource,
	requestBody common.MapStr,
) *opCreateFunction {
	return &opCreateFunction{
		log:         log,
		location:    location,
		tokenSrc:    tokenSrc,
		requestBody: requestBody,
	}
}

// Execute creates a function from the zip uploaded.
func (o *opCreateFunction) Execute(_ executor.Context) error {
	deployURL := googleAPIsURL + o.location + "/functions"

	o.log.Debugf("Posting request at %s:\n%s", deployURL, o.requestBody.StringToPrint())

	client := oauth2.NewClient(context.TODO(), o.tokenSrc)

	resp, err := client.Post(deployURL, "application/json", strings.NewReader(o.requestBody.String()))
	if err != nil {
		return err
	}

	if resp.StatusCode >= 400 {
		respTxt, err := ioutil.ReadAll(resp.Body)
		if err == nil {
			o.log.Debugf("%s", string(respTxt))
		}
		return fmt.Errorf("error while creating function: %s", resp.Status)
	}

	o.log.Debugf("Function created successfully")

	return nil
}

// Rollback removed the deployed function.
func (o *opCreateFunction) Rollback(_ executor.Context) error {
	return nil
}
