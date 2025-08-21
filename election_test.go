//go:build integration

package consul

import (
	"context"
	"testing"
	"time"

	"github.com/hashicorp/consul/api"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestClient_ElectLeader_TwoNodes(t *testing.T) {
	t.Parallel()

	logger, err := zap.NewDevelopment()
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)

	key := t.Name()

	// first node will become leader
	firstClient, err := NewClientFromConfig(&api.Config{
		Address: consulAddress,
		Scheme:  "http",
	}, WithLogger(logger.With(zap.String("name", "first"))))
	require.NoError(t, err)
	firstCtx, firstCancel := context.WithCancel(ctx)
	t.Cleanup(firstCancel)
	firstLeader, err := firstClient.ElectLeader(firstCtx, key)
	require.NoError(t, err)
	require.True(t, <-firstLeader)

	// additional node will not become leader
	secondClient, err := NewClientFromConfig(&api.Config{
		Address: consulAddress,
		Scheme:  "http",
	}, WithLogger(logger.With(zap.String("name", "second"))))
	require.NoError(t, err)
	secondCtx, secondCancel := context.WithCancel(ctx)
	t.Cleanup(secondCancel)
	secondLeader, err := secondClient.ElectLeader(secondCtx, key)
	require.NoError(t, err)
	require.False(t, <-secondLeader)

	// if first node goes down, second one will become leader instead
	firstCancel()
	_, ok := <-firstLeader
	require.False(t, ok)
	require.True(t, <-secondLeader)

	// first node rejoin will not make it a leader
	firstCtx, firstCancel = context.WithCancel(ctx)
	t.Cleanup(firstCancel)
	firstLeader, err = firstClient.ElectLeader(firstCtx, key)
	require.NoError(t, err)
	require.False(t, <-firstLeader)

	// new key will work correctly
	firstLeaderNewKey, err := firstClient.ElectLeader(firstCtx, key+"bla")
	require.NoError(t, err)
	require.True(t, <-firstLeaderNewKey)
}

func TestClient_ElectLeader_SingleNode(t *testing.T) {
	t.Parallel()

	logger, err := zap.NewDevelopment()
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)

	key := t.Name()

	// first node will become leader
	firstClient, err := NewClientFromConfig(&api.Config{
		Address: consulAddress,
		Scheme:  "http",
	}, WithLogger(logger.With(zap.String("name", "first"))))
	require.NoError(t, err)
	firstCtx, firstCancel := context.WithCancel(ctx)
	t.Cleanup(firstCancel)
	firstLeader, err := firstClient.ElectLeader(firstCtx, key)
	require.NoError(t, err)
	require.True(t, <-firstLeader)

	select {
	case <-time.After(10 * time.Second):
	case <-firstLeader:
		t.Error("didn't expect any new leaders")
		t.Fail()
	}
}
