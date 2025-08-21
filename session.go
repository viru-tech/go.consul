package consul

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/hashicorp/consul/api"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

type (
	// InfiniteSession is a session that will be
	// automatically recreated if builtin RenewPeriodic fails.
	InfiniteSession struct {
		id atomic.String
	}

	recreateSessionCallback func(sessionID string)
	destroySessionCallback  func()
)

// NewInfiniteSession returns session that will be automatically recreated
// if builtin RenewPeriodic fails. To destroy session cancel passed context.
// Passed callbacks can be used to notify caller about session changes.
// onDestroy callback is called right before the session is destroyed.
// onRecreate callback is called after session has been recreated and id changes.
func (c *Client) NewInfiniteSession(
	ctx context.Context,
	se *api.SessionEntry,
	onRecreate recreateSessionCallback,
	onDestroy destroySessionCallback,
) (*InfiniteSession, error) {
	sessionID, renewSession, err := c.NewSession(ctx, se)
	if err != nil {
		return nil, err
	}

	var session InfiniteSession
	session.id.Store(sessionID)

	go func() {
		bck := backoff.NewExponentialBackOff()

		for {
			select {
			case <-ctx.Done():
				if onDestroy != nil {
					onDestroy()
				}

				c.logger.Debug("destroying session", zap.String("session_id", session.ID()))
				if _, err := c.Session().Destroy(session.ID(), nil); err != nil {
					c.logger.Error("failed to destroy session",
						zap.String("session_id", session.ID()),
						zap.Error(err),
					)
				}
				return
			case <-renewSession:
				c.logger.Debug("creating new session")
				newSessionID, newRenewSession, err := c.NewSession(ctx, se)
				if err != nil {
					if errors.Is(err, context.Canceled) {
						renewSession = nil
						continue
					}
					c.logger.Error("failed to create new session", zap.Error(err))
					time.Sleep(bck.NextBackOff())
					continue
				}

				bck.Reset()
				session.id.Store(newSessionID)
				renewSession = newRenewSession

				if onRecreate != nil {
					onRecreate(newSessionID)
				}
			}
		}
	}()

	return &session, nil
}

// ID returns current sessionID.
func (s *InfiniteSession) ID() string {
	return s.id.String()
}

// NewSession will create new consul session with the given name that can
// be used for lead election among nomad allocations. Returned session
// will be automatically renewed until passed context is canceled or
// any error occurs. Returned channel will be closed if session cannot
// be renewed and a new one must be created or if ctx is canceled.
func (c *Client) NewSession(ctx context.Context, se *api.SessionEntry) (string, <-chan struct{}, error) {
	var opts *api.WriteOptions
	opts = opts.WithContext(ctx)

	sessionID, _, err := c.Session().Create(se, opts)
	if err != nil {
		return "", nil, fmt.Errorf("failed to create new infiniteSession: %w", err)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		c.Session().RenewPeriodic(se.TTL, sessionID, opts, ctx.Done()) //nolint:errcheck,gosec
	}()

	return sessionID, done, nil
}
