// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package gcp

import (
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"

	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/beats/x-pack/functionbeat/function/executor"
)

type opCreateFunction struct {
	log         *logp.Logger
	location    string
	requestBody common.MapStr
}

func newOpCreateFunction(log *logp.Logger, location string, requestBody common.MapStr) *opCreateFunction {
	return &opCreateFunction{log: log, requestBody: requestBody}
}

func (o *opCreateFunction) Execute(_ executor.Context) error {
	apiKey := os.Getenv("GOOGLE_CLOUD_PLATFORM_API_KEY")
	if apiKey == "" {
		return fmt.Errorf("GOOGLE_CLOUD_PLATFORM_API_KEY environment variable is not set")
	}
	params := url.Values{}
	params.Set("key", apiKey)
	deployURL := googleAPIsURL + o.location + "/functions?" + params.Encode()

	o.log.Debugf("POSTing request at %s:\n%s", deployURL, o.requestBody.StringToPrint())

	resp, err := http.Post(deployURL, "application/json", strings.NewReader(o.requestBody.String()))

	o.log.Debugf("%+v", resp)

	return err
}

func (o *opCreateFunction) Rollback(_ executor.Context) error {
	return nil
}
