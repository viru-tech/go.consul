package consul

import (
	"strconv"
	"time"
)

// Protocol to register in consul.
type Protocol string

// Available protocols.
const (
	GRPC Protocol = "grpc"
	HTTP Protocol = "http"
)

// String returns string value for protocol.
func (p Protocol) String() string {
	return string(p)
}

// Registration describes service registration in consul catalog.
type Registration struct {
	Name     string   `json:"name"`
	Protocol Protocol `json:"protocol"`
	Address  string   `json:"address"`

	Meta map[string]string `json:"meta"`
	Tags []string          `json:"tags"`

	Port int `json:"port"`

	CheckInterval   time.Duration `json:"check_interval"`
	DeregisterAfter time.Duration `json:"deregister_after"`
}

// ServiceName returns service name.
func (s *Registration) ServiceName() string {
	return makeServiceName(s.Protocol, s.Name)
}

// CheckName returns service check name.
func (s *Registration) CheckName() string {
	return s.Protocol.String() + "-" + s.Name + " check"
}

// ID returns string id that may be used as unique service id.
func (s *Registration) ID() string {
	return s.Protocol.String() + "-" + s.Name + "/" + s.Address + ":" + strconv.Itoa(s.Port)
}
