package consul

import (
	"testing"

	"github.com/hashicorp/consul/api"
	"github.com/stretchr/testify/require"
)

//nolint:paralleltest,tparallel
func TestClient_nomad(t *testing.T) {
	t.Parallel()

	client, err := NewClientFromConfig(&api.Config{
		Address: consulAddress,
		Scheme:  "http",
	})
	require.NoError(t, err)

	const (
		allocID  = "5fade085-d620-84f1-7a20-589eb061834a"
		taskName = "go-feed"
	)

	var (
		tags               = []string{"staging", "staging3", "green", "feature-RTB-510-v1"}
		serviceIDToService = make(map[string]*api.AgentService)
		httpServiceName    = makeServiceName(HTTP, taskName)
		grpcServiceName    = makeServiceName(GRPC, taskName)
	)

	t.Run("services not registered", func(t *testing.T) {
		ss, err := client.getAllocServices(t.Context(), "", taskName, allocID)
		require.NoError(t, err)
		require.Empty(t, ss)
	})

	t.Run("services found", func(t *testing.T) {
		err := client.Agent().ServiceRegister(&api.AgentServiceRegistration{
			ID:   makeAllocServiceID(allocID, taskName, httpServiceName, HTTP.String()),
			Name: httpServiceName,
			Tags: tags,
			Meta: map[string]string{
				"foo": "bar",
			},
			Address: "127.0.0.1",
			Port:    80,
			Checks: []*api.AgentServiceCheck{
				{
					CheckID:                        "http-check-id",
					Name:                           "http-check",
					HTTP:                           "http://localhost:8080",
					Interval:                       "2m",
					DeregisterCriticalServiceAfter: "5m",
				},
			},
		})
		require.NoError(t, err)

		err = client.Agent().ServiceRegister(&api.AgentServiceRegistration{
			ID:   makeAllocServiceID(allocID, taskName, grpcServiceName, GRPC.String()),
			Name: grpcServiceName,
			Tags: tags,
			Meta: map[string]string{
				"bar": "foo",
			},
			Address: "127.0.0.1",
			Port:    81,
			Checks: []*api.AgentServiceCheck{
				{
					CheckID:                        "grpc-check-id",
					Name:                           "grpc-check",
					GRPC:                           "localhost:8081",
					Interval:                       "2m",
					DeregisterCriticalServiceAfter: "5m",
				},
			},
		})
		require.NoError(t, err)

		ss, err := client.getAllocServices(t.Context(), "", taskName, allocID)
		require.NoError(t, err)
		require.Len(t, ss, 2)

		for _, s := range ss {
			serviceIDToService[s.ID] = s
		}

		httpChecks, _, err := client.Health().Service(httpServiceName, "", false, nil)
		require.NoError(t, err)
		require.Len(t, httpChecks, 1)

		grpcChecks, _, err := client.Health().Service(grpcServiceName, "", false, nil)
		require.NoError(t, err)
		require.Len(t, grpcChecks, 1)
	})

	t.Run("services not found", func(t *testing.T) {
		ss, err := client.getAllocServices(t.Context(), "", taskName, "foobar")
		require.NoError(t, err)
		require.Empty(t, ss)
	})

	t.Run("append tags", func(t *testing.T) {
		err := client.AppendNomadTaskServiceTags(t.Context(), "", taskName, allocID, "leader")
		require.NoError(t, err)

		ss, err := client.getAllocServices(t.Context(), "", taskName, allocID)
		require.NoError(t, err)
		require.Len(t, ss, 2)

		for _, s := range ss {
			require.ElementsMatch(t, s.Tags, append(tags, "leader"))
			s.Tags = tags
			compareAgentServiceWithRegistration(t, s, serviceIDToService[s.ID])
		}

		httpChecks, _, err := client.Health().Service(httpServiceName, "", false, nil)
		require.NoError(t, err)
		require.Len(t, httpChecks, 1)

		grpcChecks, _, err := client.Health().Service(grpcServiceName, "", false, nil)
		require.NoError(t, err)
		require.Len(t, grpcChecks, 1)
	})

	t.Run("append tags noop", func(t *testing.T) {
		err := client.AppendNomadTaskServiceTags(t.Context(), "", taskName, allocID, "leader")
		require.NoError(t, err)

		ss, err := client.getAllocServices(t.Context(), "", taskName, allocID)
		require.NoError(t, err)
		require.Len(t, ss, 2)
	})

	t.Run("remove tags", func(t *testing.T) {
		err := client.RemoveTaskServiceTags(t.Context(), "", taskName, allocID, "leader")
		require.NoError(t, err)

		ss, err := client.getAllocServices(t.Context(), "", taskName, allocID)
		require.NoError(t, err)
		require.Len(t, ss, 2)

		for _, s := range ss {
			compareAgentServiceWithRegistration(t, s, serviceIDToService[s.ID])
		}

		httpChecks, _, err := client.Health().Service(httpServiceName, "", false, nil)
		require.NoError(t, err)
		require.Len(t, httpChecks, 1)

		grpcChecks, _, err := client.Health().Service(grpcServiceName, "", false, nil)
		require.NoError(t, err)
		require.Len(t, grpcChecks, 1)
	})

	t.Run("remove tags noop", func(t *testing.T) {
		err := client.RemoveTaskServiceTags(t.Context(), "", taskName, allocID, "leader")
		require.NoError(t, err)

		ss, err := client.getAllocServices(t.Context(), "", taskName, allocID)
		require.NoError(t, err)
		require.Len(t, ss, 2)
	})
}

func TestSubtractTags(t *testing.T) {
	t.Parallel()

	tt := []struct {
		in     []string
		sub    []string
		expect []string
	}{
		{
			in:     []string{"staging", "staging3", "green", "feature-RTB-510-v1"},
			sub:    []string{"leader"},
			expect: []string{"staging", "staging3", "green", "feature-RTB-510-v1"},
		},
		{
			in:     []string{"staging", "staging3", "green", "feature-RTB-510-v1", "leader"},
			sub:    []string{"leader"},
			expect: []string{"staging", "staging3", "green", "feature-RTB-510-v1"},
		},
		{
			in:     []string{"staging", "staging3", "green", "leader", "feature-RTB-510-v1"},
			sub:    []string{"leader"},
			expect: []string{"staging", "staging3", "green", "feature-RTB-510-v1"},
		},
		{
			in:     []string{"staging", "staging3", "green", "leader", "feature-RTB-510-v1", "leader"},
			sub:    []string{"leader"},
			expect: []string{"staging", "staging3", "green", "feature-RTB-510-v1"},
		},
		{
			in:     []string{"leader", "staging", "staging3", "green", "feature-RTB-510-v1"},
			sub:    []string{"leader"},
			expect: []string{"staging", "staging3", "green", "feature-RTB-510-v1"},
		},
	}

	for i := range tt {
		tc := tt[i]
		t.Run("", func(t *testing.T) {
			t.Parallel()

			actual := subtractTags(tc.in, tc.sub)
			require.Equal(t, tc.expect, actual)
		})
	}
}

func compareAgentServiceWithRegistration(t *testing.T, got, want *api.AgentService) {
	t.Helper()

	require.Equal(t, want.Kind, got.Kind)
	require.Equal(t, want.ID, got.ID)
	require.Equal(t, want.Service, got.Service)
	require.Equal(t, want.Tags, got.Tags)
	require.Equal(t, want.Port, got.Port)
	require.Equal(t, want.Address, got.Address)
	require.Equal(t, want.SocketPath, got.SocketPath)
	require.Equal(t, want.TaggedAddresses, got.TaggedAddresses)
	require.Equal(t, want.EnableTagOverride, got.EnableTagOverride)
	require.Equal(t, want.Meta, got.Meta)
	require.Equal(t, want.Weights, got.Weights)
	require.Equal(t, want.Proxy, got.Proxy)
	require.Equal(t, want.Connect, got.Connect)
	require.Equal(t, want.Namespace, got.Namespace)
}
