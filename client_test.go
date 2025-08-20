//go:build integration

package consul

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

var consulAddress string

func TestMain(m *testing.M) {
	cleanup, err := setupConsul()
	if err != nil {
		log.Fatalf("failed to setup consul: %v", err)
	}

	code := m.Run()

	cleanup()
	os.Exit(code)
}

func setupConsul() (func(), error) {
	req := testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "consul:1.14.2",
			ExposedPorts: []string{"8500/tcp"},
			WaitingFor: wait.ForAll(
				wait.ForListeningPort("8500/tcp"),
				wait.ForLog("agent: Synced node info"),
			),
		},
		Started: true,
	}

	consulContainer, err := testcontainers.GenericContainer(context.Background(), req)
	if err != nil {
		return nil, fmt.Errorf("failed to start consul container: %w", err)
	}

	cleanup := func() {
		_ = consulContainer.Terminate(context.Background())
	}

	consulAddress, err = consulContainer.PortEndpoint(context.Background(), "8500", "")
	if err != nil {
		cleanup()
		return nil, fmt.Errorf("failed to get consul address: %w", err)
	}

	return cleanup, nil
}
