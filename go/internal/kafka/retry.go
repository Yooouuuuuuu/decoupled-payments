package kafka

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// unreachableRetry is how long a role keeps trying while it can't reach the
// brokers, like the Java client's default.api.timeout.ms: a process started a
// moment before the brokers, or during a broker restart, waits instead of
// failing at once. (franz-go fails fast on a single bootstrap broker that
// refuses connections.)
const unreachableRetry = 60 * time.Second

// AwaitBrokers waits until a broker answers, for at most a minute.
func AwaitBrokers(ctx context.Context, cl *kgo.Client, log *slog.Logger) error {
	return retryUnreachable(ctx, log, func() error { return cl.Ping(ctx) })
}

// retryUnreachable calls fn until it succeeds, fails for another reason
// than unreachable brokers, or a minute has passed.
func retryUnreachable(ctx context.Context, log *slog.Logger, fn func() error) error {
	deadline := time.Now().Add(unreachableRetry)
	for logged := false; ; logged = true {
		err := fn()
		switch {
		case err != nil && ctx.Err() != nil:
			return ctx.Err() // stopped, however the client reported it
		case err == nil || !unreachable(err):
			return err
		}
		if !time.Now().Before(deadline) {
			return fmt.Errorf("no broker could be reached for %v: %w", unreachableRetry, err)
		}
		if !logged {
			log.Warn(fmt.Sprintf("can't reach the brokers yet, trying again for up to %v", unreachableRetry), "err", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

// unreachable reports whether err means that the brokers can't be reached
// or aren't ready yet: a refused or broken connection, or a retriable broker
// error such as a partition without a leader. A topic that doesn't exist is
// not: waiting doesn't create it.
func unreachable(err error) bool {
	var netErr *net.OpError
	switch {
	case errors.Is(err, kerr.UnknownTopicOrPartition):
		return false
	case errors.As(err, &netErr), errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF):
		return true
	}
	return kerr.IsRetriable(err) || kgo.IsRetryableBrokerErr(err)
}
