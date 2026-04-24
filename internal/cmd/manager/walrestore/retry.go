/*
Copyright © contributors to CloudNativePG, established as
CloudNativePG a Series of LF Projects, LLC.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

SPDX-License-Identifier: Apache-2.0
*/

package walrestore

import (
	"context"
	"errors"
	"fmt"
	"time"

	barmanRestorer "github.com/cloudnative-pg/barman-cloud/pkg/restorer"
	"github.com/cloudnative-pg/machinery/pkg/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ErrRetryTimeoutReached is returned when the retry loop has exhausted its
// configurable budget without downloading the WAL. The caller is expected to
// translate this into exit code 255 so that PostgreSQL stops log-shipping
// replication instead of promoting itself prematurely.
var ErrRetryTimeoutReached = errors.New("retry timeout reached while restoring WAL")

// DefaultMaxRetryTimeout is the default budget for retrying transient
// WAL-restore failures. After this deadline the command will ask PostgreSQL
// to stop replication (exit 255).
const DefaultMaxRetryTimeout = 5 * time.Minute

// retryBackoffCap is the maximum interval between retry attempts. We start
// from a small interval and grow up to this cap.
//
// Declared as vars (not consts) so tests can shrink them for fast execution.
var retryBackoffCap = 30 * time.Second

// retryBackoffInitial is the interval between the first and the second
// attempt. Subsequent intervals grow exponentially up to retryBackoffCap.
var retryBackoffInitial = 1 * time.Second

// classifyPluginError reports whether an error returned from a CNPG-i plugin
// should be treated as a definitive "WAL not found" (no retry) or as a
// transient failure worth retrying.
//
// Any error containing a gRPC status with codes.NotFound is treated as
// "not found". Everything else is transient: we would rather wait out a
// flaky network than let PostgreSQL promote on an incomplete archive.
func classifyPluginError(err error) restoreErrorKind {
	if err == nil {
		return restoreErrorNone
	}
	if st, ok := status.FromError(err); ok && st.Code() == codes.NotFound {
		return restoreErrorNotFound
	}
	return restoreErrorTransient
}

// classifyBarmanError mirrors classifyPluginError for the in-tree barman-cloud
// path, which already exposes ErrWALNotFound as its sentinel for exit code 1.
func classifyBarmanError(err error) restoreErrorKind {
	if err == nil {
		return restoreErrorNone
	}
	if errors.Is(err, barmanRestorer.ErrWALNotFound) {
		return restoreErrorNotFound
	}
	return restoreErrorTransient
}

// restoreErrorKind categorizes the outcome of a single restore attempt so the
// retry loop can decide whether to exit immediately, retry, or surface a
// timeout.
type restoreErrorKind int

const (
	// restoreErrorNone means the attempt succeeded.
	restoreErrorNone restoreErrorKind = iota
	// restoreErrorNotFound means the WAL is definitively absent. Retrying
	// won't change that, and PostgreSQL's own retry mechanics already cover
	// the log-shipping "advance to streaming" transition.
	restoreErrorNotFound
	// restoreErrorTransient means the attempt failed for a reason that might
	// succeed on retry (network blip, transient cloud provider error, etc.).
	restoreErrorTransient
)

// nextBackoff returns the next retry interval, capped at retryBackoffCap.
func nextBackoff(current time.Duration) time.Duration {
	if current <= 0 {
		return retryBackoffInitial
	}
	next := current * 2
	if next > retryBackoffCap {
		return retryBackoffCap
	}
	return next
}

// attemptFunc is the unit of work the retry loop drives. Decoupling the loop
// from the concrete restore implementation keeps the loop unit-testable.
type attemptFunc func(ctx context.Context) error

// retryUntilDeadline runs attempt repeatedly until it returns a non-transient
// result, the context is canceled, or the deadline is reached. Sleeps between
// attempts grow exponentially up to retryBackoffCap.
//
// Returns:
//   - whatever attempt returned (success or non-transient sentinel) if the
//     loop terminated naturally,
//   - ErrRetryTimeoutReached (wrapping the last transient error) if the
//     deadline was hit or the context was canceled mid-wait.
//
// The first call to attempt is made unconditionally, before any sleep, so
// that a happy first attempt incurs zero retry overhead.
func retryUntilDeadline(
	ctx context.Context,
	attempt attemptFunc,
	deadline time.Time,
) error {
	contextLog := log.FromContext(ctx)

	err := attempt(ctx)
	if !isTransientRestoreError(err) {
		return err
	}

	backoff := nextBackoff(0)
	attemptCount := 1
	for {
		if !time.Now().Add(backoff).Before(deadline) {
			return fmt.Errorf("%w after %d attempt(s): last error: %w",
				ErrRetryTimeoutReached, attemptCount, err)
		}
		contextLog.Info("transient WAL restore error, will retry",
			"attempt", attemptCount, "nextRetryIn", backoff, "deadline", deadline,
			"error", err)
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return fmt.Errorf("%w: context canceled while retrying: %w: %w",
				ErrRetryTimeoutReached, ctx.Err(), err)
		}
		attemptCount++
		err = attempt(ctx)
		if !isTransientRestoreError(err) {
			return err
		}
		backoff = nextBackoff(backoff)
	}
}
