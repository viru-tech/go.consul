package consul

import (
	"context"
	"fmt"
	"strings"

	"github.com/hashicorp/consul/api"
)

// AppendNomadTaskServiceTags appends passed tags to nomad tasks' services.
// Note: you must set enable_tags_override=true for changes to persist.
func (c *Client) AppendNomadTaskServiceTags(
	ctx context.Context,
	dc string,
	taskName string,
	allocID string,
	tags ...string,
) error {
	ss, err := c.getAllocServices(ctx, dc, taskName, allocID)
	if err != nil {
		return err
	}

	for _, s := range ss {
		diff := subtractTags(tags, s.Tags)
		if len(diff) == 0 {
			continue
		}

		if err := c.updateServiceTags(ctx, s, append(s.Tags, diff...)); err != nil {
			return err
		}
	}

	return nil
}

// RemoveTaskServiceTags removes passed tags from nomad tasks' services.
// Note: you must set enable_tags_override=true for changes to persis.
func (c *Client) RemoveTaskServiceTags(
	ctx context.Context,
	dc string,
	taskName string,
	allocID string,
	tags ...string,
) error {
	ss, err := c.getAllocServices(ctx, dc, taskName, allocID)
	if err != nil {
		return err
	}

	for _, s := range ss {
		newTags := subtractTags(s.Tags, tags)
		if len(newTags) == len(s.Tags) {
			continue
		}

		if err := c.updateServiceTags(ctx, s, newTags); err != nil {
			return err
		}
	}

	return nil
}

func (c *Client) updateServiceTags(ctx context.Context, s *api.AgentService, newTags []string) error {
	var wOpts api.ServiceRegisterOpts
	wOpts = wOpts.WithContext(ctx)

	err := c.Agent().ServiceRegisterOpts(&api.AgentServiceRegistration{
		Kind:              s.Kind,
		ID:                s.ID,
		Name:              s.Service,
		Tags:              newTags,
		Port:              s.Port,
		Address:           s.Address,
		SocketPath:        s.SocketPath,
		TaggedAddresses:   s.TaggedAddresses,
		EnableTagOverride: s.EnableTagOverride,
		Meta:              s.Meta,
		Weights:           &s.Weights,
		Proxy:             s.Proxy,
		Connect:           s.Connect,
		Namespace:         s.Namespace,
	}, wOpts)
	if err != nil {
		return fmt.Errorf("failed to update service tags: %w", err)
	}

	return nil
}

// getAllocServices returns http and grpc services registered for the nomad allocation.
func (c *Client) getAllocServices(
	ctx context.Context,
	dc string,
	taskName string,
	allocID string,
) ([]*api.AgentService, error) {
	q := &api.QueryOptions{Datacenter: dc}
	q = q.WithContext(ctx)

	var res []*api.AgentService

	httpServiceID := makeAllocServiceID(allocID, taskName, makeServiceName(HTTP, taskName), HTTP.String())
	httpService, _, err := c.Agent().Service(httpServiceID, q)
	if err != nil {
		if !strings.Contains(err.Error(), "Unexpected response code: 404") {
			return nil, fmt.Errorf("failed to get %q service: %w", httpServiceID, err)
		}
	} else {
		res = append(res, httpService)
	}

	grpcServiceID := makeAllocServiceID(allocID, taskName, makeServiceName(GRPC, taskName), GRPC.String())
	grpcService, _, err := c.Agent().Service(grpcServiceID, q)
	if err != nil {
		if !strings.Contains(err.Error(), "Unexpected response code: 404") {
			return nil, fmt.Errorf("failed to get %q service: %w", grpcServiceID, err)
		}
	} else {
		res = append(res, grpcService)
	}

	return res, nil
}

func subtractTags(tags, withoutTags []string) []string {
	for _, without := range withoutTags {
		for i := 0; i < len(tags); {
			if tags[i] == without {
				tags = append(tags[:i], tags[i+1:]...)
				continue
			}
			i++
		}
	}

	return tags
}

// makeAllocServiceID creates a unique ID for identifying an alloc service in Consul.
// Taken from nomad source code. Works correctly for nomad v0.7.1+.
// Example Service ID: _nomad-task-b4e61df9-b095-d64e-f241-23860da1375f-redis-http-http.
func makeAllocServiceID(
	allocID string,
	taskName string,
	serviceName string,
	portLabel string,
) string {
	return fmt.Sprintf("_nomad-task-%s-%s-%s-%s", allocID, taskName, serviceName, portLabel)
}
