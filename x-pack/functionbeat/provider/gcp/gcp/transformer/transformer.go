// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package transformer

import (
	"context"
	"time"

	"cloud.google.com/go/functions/metadata"
	"cloud.google.com/go/pubsub"

	"github.com/elastic/beats/libbeat/beat"
	"github.com/elastic/beats/libbeat/common"
)

// PubSub takes a Pub/Sub message and context and transforms it into an event.
func PubSub(ctx context.Context, msg pubsub.Message) (beat.Event, error) {
	mData, err := metadata.FromContext(ctx)
	if err != nil {
		return beat.Event{}, err
	}

	return beat.Event{
		Timestamp: mData.Timestamp,
		Fields: common.MapStr{
			"read_timestamp": time.Now(),
			"message":        string(msg.Data),
			"attributes":     msg.Attributes,
			"id":             mData.EventID,
			"resource": common.MapStr{
				"service":    mData.Resource.Service,
				"name":       mData.Resource.Name,
				"event_type": mData.Resource.Type,
			},
		},
	}, nil
}

// CloudStorage takes a Cloud Storage object and transforms it into an event.
func CloudStorage(ctx context.Context) (beat.Event, error) {
	return beat.Event{}, nil
}
