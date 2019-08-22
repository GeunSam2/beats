// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package gcp

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"

	"golang.org/x/oauth2"

	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/beats/x-pack/functionbeat/manager/executor"
)

type opDeleteFunction struct {
	log      *logp.Logger
	location string
	name     string
	tokenSrc oauth2.TokenSource
}

func newOpDeleteFunction(
	log *logp.Logger,
	location string,
	name string,
	tokenSrc oauth2.TokenSource,
) *opDeleteFunction {
	return &opDeleteFunction{
		log:      log,
		location: location,
		name:     name,
		tokenSrc: tokenSrc,
	}
}

// Execute creates a function from the zip uploaded.
func (o *opDeleteFunction) Execute(_ executor.Context) error {
	deleteURL := googleAPIsURL + o.location + "/functions/" + o.name

	o.log.Debugf("Deleting function at %s", deleteURL)

	client := oauth2.NewClient(context.TODO(), o.tokenSrc)

	req, err := http.NewRequest("DELETE", deleteURL, nil)
	if err != nil {
		return err
	}

	resp, err := client.Do(req)
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

	o.log.Debugf("Function removed successfully")

	return nil
}

// Rollback
func (o *opDeleteFunction) Rollback(_ executor.Context) error {
	return nil
}
