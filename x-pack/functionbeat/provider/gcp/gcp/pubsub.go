// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package gcp

import (
	"context"
	"fmt"

	"cloud.google.com/go/pubsub"

	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/beats/x-pack/functionbeat/function/core"
	"github.com/elastic/beats/x-pack/functionbeat/function/provider"
	"github.com/elastic/beats/x-pack/functionbeat/provider/gcp/gcp/transformer"
)

// PubSub represents a Google Cloud function which reads event from Google Pub/Sub triggers.
type PubSub struct {
	config *functionConfig
	log    *logp.Logger
}

type PubSubMsg string
type PubSubContext string

// NewPubSub returns a new function to read from Google Pub/Sub.
func NewPubSub(provider provider.Provider, config *common.Config) (provider.Function, error) {
	functionConfig := &functionConfig{}
	if err := config.Unpack(functionConfig); err != nil {
		return nil, err
	}
	return &PubSub{
		config: functionConfig,
		log:    logp.NewLogger("pubsub"),
	}, nil
}

// Run start
func (c *PubSub) Run(ctx context.Context, client core.Client) error {
	msgCtx, msg, err := c.getEventDataFromContext(ctx)
	if err != nil {
		return err
	}
	c.log.Debugf("Message context: +%+v", msgCtx)
	c.log.Debugf("Message: +%+v", msg)

	event, err := transformer.PubSub(msgCtx, msg)
	c.log.Debug(">>>> %+v", event)
	if err := client.Publish(event); err != nil {
		c.log.Errorf("error while publishing Pub/Sub event %+v", err)
		return err
	}
	client.Wait()

	return nil
}

func (c *PubSub) getEventDataFromContext(ctx context.Context) (context.Context, pubsub.Message, error) {
	iMsgCtx := ctx.Value(PubSubContext("pub_sub_context"))
	if iMsgCtx == nil {
		return nil, pubsub.Message{}, fmt.Errorf("no pub/sub message context")
	}
	msgCtx, ok := iMsgCtx.(context.Context)
	if !ok {
		return nil, pubsub.Message{}, fmt.Errorf("not message context: %+v", iMsgCtx)
	}

	iMsg := ctx.Value(PubSubMsg("pub_sub_message"))
	if iMsg == nil {
		return nil, pubsub.Message{}, fmt.Errorf("no pub/sub message")
	}
	msg, ok := iMsg.(pubsub.Message)
	if !ok {
		return nil, pubsub.Message{}, fmt.Errorf("not message: %+v", iMsg)
	}
	return msgCtx, msg, nil
}

// Name returns the name of the function.
func (p *PubSub) Name() string {
	return "pubsub"
}
