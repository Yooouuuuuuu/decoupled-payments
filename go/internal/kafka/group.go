package kafka

import (
	"context"
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// refusals are the consumer group errors that rejoining can't fix: the
// configuration or the permissions are wrong, or the group is full. franz-go
// reports them in a poll and rejoins forever; the Java client fails. A
// member dropped from the group after a pause (UNKNOWN_MEMBER_ID,
// ILLEGAL_GENERATION, REBALANCE_IN_PROGRESS) just rejoins.
var refusals = []error{
	kerr.InvalidSessionTimeout, // kafka.sessionTimeoutMs is outside the broker's group.min/max.session.timeout.ms
	kerr.GroupAuthorizationFailed,
	kerr.InconsistentGroupProtocol, // another member uses another protocol or assignor
	kerr.InvalidGroupID,
	kerr.GroupMaxSizeReached,
}

// GroupRefused returns a fatal error if err, the errors of a poll, says that
// the consumer group won't take this member: one of refusals, or another
// process with this siteId took its place (FENCED_INSTANCE_ID, and rejoining
// would only fence that process in turn). Otherwise it returns nil.
func GroupRefused(err error, siteID int) error {
	if errors.Is(err, kerr.FencedInstanceID) {
		return fmt.Errorf("another process uses siteId %d: %w", siteID, err)
	}
	for _, refusal := range refusals {
		if errors.Is(err, refusal) {
			return fmt.Errorf("the consumer group refused this process: %w", err)
		}
	}
	return nil
}

// LeaveGroup makes a static member leave its consumer group now, after a
// clean stop, so that its partitions move at once and init --reset works
// right away. franz-go never sends a LeaveGroup for a static member
// (kgo.InstanceID): after a crash or a failure the member stays, and a
// restart within the session timeout gets its partitions back without a
// rebalance.
//
// It first stops the member's group management, which runs the final
// revoke callbacks, then sends the LeaveGroup with the same client. The
// caller then closes the client.
func LeaveGroup(ctx context.Context, cl *kgo.Client, group, instanceID string) error {
	cl.AllowRebalance()
	if err := cl.LeaveGroupContext(ctx); err != nil {
		return err
	}
	left, err := kadm.NewClient(cl).LeaveGroup(ctx, kadm.LeaveGroup(group).InstanceIDs(instanceID).Reason("stopped"))
	if err == nil {
		err = left.Error()
	}
	if errors.Is(err, kerr.UnknownMemberID) {
		return nil // the group had already dropped this member
	}
	return err
}
