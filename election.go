package consul

import (
	"context"
	"errors"
	"time"

	"github.com/hashicorp/consul/api"
	"go.uber.org/zap"
)

// ElectLeader enters leader election algorithm for the resource named key.
// It returns channel where caller's state will be passed (true - entered as
// leader, false - entered as peer). This method also watches for any election
// changes and in case of caller's state change new state will be pushed into
// channel. To quit election process, cancel the passed context.
//
//nolint:gocognit
func (c *Client) ElectLeader(ctx context.Context, key string) (<-chan bool, error) {
	isLeader := make(chan bool, 1)

	const (
		sessionTTL = "30s"
		lockDelay  = 15 * time.Second
	)

	sessionEntry := &api.SessionEntry{
		Name:      key,
		Behavior:  "release",
		TTL:       sessionTTL,
		LockDelay: lockDelay,
	}

	// note: one cannot use infiniteSession here b/c lock modifications
	// from different goroutines are potential race conditions.
	sessionID, renewSession, err := c.NewSession(ctx, sessionEntry)
	if err != nil {
		return nil, err
	}

	go func() {
		defer close(isLeader)

		lock := &api.KVPair{
			Key:     key,
			Value:   []byte(sessionID),
			Session: sessionID,
			Flags:   api.LockFlagValue,
		}

		wOpts := new(api.WriteOptions).WithContext(ctx)
		qOpt := new(api.QueryOptions).WithContext(ctx)

		newElection := make(chan struct{}, 1)
		newElection <- struct{}{}

		for {
			select {
			case <-ctx.Done():
				c.teardownLeaderElection(lock)
				return
			case <-newElection:
				ok, _, err := c.KV().Acquire(lock, wOpts)
				if err != nil {
					c.logger.Error("failed to acquire lock",
						zap.String("session_id", lock.Session),
						zap.Error(err),
					)
					newElection <- struct{}{}
					continue
				}

				c.logger.Debug("election finished",
					zap.String("key", lock.Key),
					zap.String("session_id", lock.Session),
					zap.Bool("is_leader", ok),
				)

				isLeader <- ok
				qOpt.WaitIndex = lock.ModifyIndex
			case <-renewSession:
				c.logger.Debug("creating new session")
				newSessionID, newRenewSession, err := c.NewSession(ctx, sessionEntry)
				if err != nil {
					if errors.Is(err, context.Canceled) {
						renewSession = nil
						continue
					}
					c.logger.Error("failed to create new session", zap.Error(err))
					continue
				}
				lock.Session = newSessionID
				lock.Value = []byte(newSessionID)
				renewSession = newRenewSession
			default:
				newLockKey, meta, err := c.KV().Get(key, qOpt)
				if err != nil {
					if errors.Is(err, context.Canceled) {
						continue
					}
					c.logger.Error("failed to watch lock",
						zap.String("session_id", lock.Session),
						zap.Error(err),
					)
					continue
				}

				if meta.LastIndex == qOpt.WaitIndex {
					c.logger.Debug("no change in lock key",
						zap.Uint64("modify_index", meta.LastIndex),
						zap.String("key", lock.Key),
						zap.String("session_id", lock.Session),
					)
					continue
				}

				if meta.LastIndex < qOpt.WaitIndex {
					// according to https://www.consul.io/api-docs/features/blocking
					// we should reset the index if it goes backward
					c.logger.Debug("resetting wait index for lock key",
						zap.String("key", lock.Key),
						zap.String("session_id", lock.Session),
					)

					qOpt.WaitIndex = 0
					lock.ModifyIndex = 0
				} else {
					c.logger.Debug("lock key updated",
						zap.Uint64("modify_index", meta.LastIndex),
						zap.String("key", lock.Key),
						zap.String("session_id", lock.Session),
					)
					qOpt.WaitIndex = meta.LastIndex
					lock.ModifyIndex = meta.LastIndex
				}

				if newLockKey == nil || newLockKey.Session == "" {
					c.logger.Debug("lock is released, starting new election after lock delay",
						zap.Duration("lock_delay", lockDelay),
						zap.String("session_id", lock.Session),
					)
					time.Sleep(lockDelay)
					newElection <- struct{}{}
				}
			}
		}
	}()

	return isLeader, nil
}

func (c *Client) teardownLeaderElection(lock *api.KVPair) {
	if c.releaseElectionLock(lock) {
		c.deleteElectionLock(lock)
	}

	c.logger.Debug("destroying session",
		zap.String("key", lock.Key),
		zap.String("session_id", lock.Session),
	)

	if _, err := c.Session().Destroy(lock.Session, nil); err != nil {
		c.logger.Error("failed to destroy session",
			zap.String("session_id", lock.Session),
			zap.Error(err),
		)
	}
}

// releaseElectionLock will release lock only if it
// is held by the session in the KVPair. Returns true
// if lock has been held by current session.
func (c *Client) releaseElectionLock(lock *api.KVPair) bool {
	c.logger.Debug("attempting to release lock",
		zap.String("key", lock.Key),
		zap.String("session_id", lock.Session),
	)

	ok, _, err := c.KV().Release(lock, nil)
	if err != nil {
		c.logger.Error("failed to release lock",
			zap.String("key", lock.Key),
			zap.String("session_id", lock.Session),
			zap.Error(err),
		)
		return false
	}

	msg := "lock is not released"
	if ok {
		msg = "lock is released"
	}
	c.logger.Debug(msg,
		zap.String("key", lock.Key),
		zap.String("session_id", lock.Session),
	)

	return ok
}

// deleteElectionLock will delete lock in a CAS manner, i.e.
// only if ModifyIndex is the same as in lock.ModifyIndex.
func (c *Client) deleteElectionLock(lock *api.KVPair) {
	_, meta, err := c.KV().Get(lock.Key, nil)
	if err != nil {
		c.logger.Error("failed to get lock before removal",
			zap.String("key", lock.Key),
			zap.String("session_id", lock.Session),
			zap.Error(err),
		)
		return
	}

	lock.ModifyIndex = meta.LastIndex
	c.logger.Debug("attempting to delete lock",
		zap.Uint64("modify_index", lock.ModifyIndex),
		zap.String("key", lock.Key),
		zap.String("session_id", lock.Session),
	)

	ok, _, err := c.KV().DeleteCAS(lock, nil)
	if err != nil {
		c.logger.Error("failed to delete lock",
			zap.Uint64("modify_index", lock.ModifyIndex),
			zap.String("key", lock.Key),
			zap.String("session_id", lock.Session),
			zap.Error(err),
		)
		return
	}

	msg := "lock is not deleted"
	if ok {
		msg = "lock is deleted"
	}
	c.logger.Debug(msg,
		zap.Uint64("modify_index", lock.ModifyIndex),
		zap.String("key", lock.Key),
		zap.String("session_id", lock.Session),
	)
}
