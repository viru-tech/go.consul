package consul

import (
	"strconv"

	"github.com/hashicorp/consul/api"
	"go.uber.org/zap"
)

// Option configures Client.
type Option func(c *Client)

// WithLogger sets passed logger.
func WithLogger(l *zap.Logger) Option {
	return func(c *Client) {
		c.logger = l
	}
}

// Client is a wrapper over consul client.
type Client struct {
	*api.Client
	logger *zap.Logger
}

// NewClient creates new client for future work with consul service discovery and KV storage.
func NewClient(scheme, host string, port int, dc string) (*Client, error) {
	return NewClientFromConfig(&api.Config{
		Address:    host + ":" + strconv.Itoa(port),
		Scheme:     scheme,
		Datacenter: dc,
	})
}

// NewClientFromConfig creates new client with the given configuration
// for future work with consul service discovery and KV storage.
func NewClientFromConfig(conf *api.Config, opts ...Option) (*Client, error) {
	consulClient, err := api.NewClient(conf)
	if err != nil {
		return nil, err
	}

	c := &Client{
		logger: zap.NewNop(),
		Client: consulClient,
	}

	for _, o := range opts {
		o(c)
	}

	return c, nil
}

// Host returns consul agent's host.
func (c *Client) Host() (string, error) {
	self, err := c.Agent().Self()
	if err != nil {
		return "", err
	}

	return self["Member"]["Addr"].(string), nil //nolint:forcetypeassert,errcheck
}

// ServiceRegister registers new service in consul catalog.
func (c *Client) ServiceRegister(registration *Registration) (string, error) {
	asc := &api.AgentServiceCheck{
		CheckID: registration.ID(),
		Name:    registration.CheckName(),

		Interval:                       registration.CheckInterval.String(),
		DeregisterCriticalServiceAfter: registration.DeregisterAfter.String(),
	}

	switch registration.Protocol {
	case GRPC:
		asc.GRPC = registration.Address + ":" + strconv.Itoa(registration.Port)
	case HTTP:
		asc.HTTP = "http://" + registration.Address + ":" + strconv.Itoa(registration.Port) + "/check"
	}

	asr := &api.AgentServiceRegistration{
		ID:   registration.ID(),
		Name: registration.ServiceName(),

		Tags: registration.Tags,
		Meta: registration.Meta,

		Address: registration.Address,
		Port:    registration.Port,

		Check: asc,
	}

	if err := c.Agent().ServiceRegister(asr); err != nil {
		return "", err
	}

	return asr.ID, nil
}

// ServiceDeregister deregisters service from the consul catalog.
func (c *Client) ServiceDeregister(id string) error {
	if err := c.Agent().ServiceDeregister(id); err != nil {
		return err
	}

	return nil
}

// ServiceAddresses returns slice of addresses from consul catalog with provided service name and tag.
func (c *Client) ServiceAddresses(name string, tags []string, dc string) ([]string, error) {
	q := &api.QueryOptions{
		Datacenter: dc,
	}

	catalogs, _, err := c.Health().ServiceMultipleTags(name, tags, true, q)
	if err != nil {
		return nil, err
	}

	addresses := make([]string, 0, len(catalogs))
	for _, catalog := range catalogs {
		if len(catalog.Service.Address) == 0 {
			catalog.Service.Address = catalog.Node.Address
		}

		addresses = append(addresses, catalog.Service.Address+":"+strconv.Itoa(catalog.Service.Port))
	}

	return addresses, nil
}

func makeServiceName(proto Protocol, taskName string) string {
	return proto.String() + "-" + taskName
}
