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
	"sync/atomic"
	"time"

	barmanRestorer "github.com/cloudnative-pg/barman-cloud/pkg/restorer"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	apiv1 "github.com/cloudnative-pg/cloudnative-pg/api/v1"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("classifyPluginError", func() {
	DescribeTable("classifies gRPC and non-gRPC errors",
		func(err error, expected restoreErrorKind) {
			Expect(classifyPluginError(err)).To(Equal(expected))
		},
		Entry("nil is None", nil, restoreErrorNone),
		Entry("gRPC NotFound is NotFound",
			status.Error(codes.NotFound, "wal not in archive"), restoreErrorNotFound),
		Entry("NotFound through fmt.Errorf %w is still NotFound",
			fmt.Errorf("plugin foo: %w", status.Error(codes.NotFound, "x")), restoreErrorNotFound),
		Entry("any other gRPC code is Transient",
			status.Error(codes.Unavailable, "dial tcp"), restoreErrorTransient),
		Entry("a plain error is Transient",
			errors.New("something blew up"), restoreErrorTransient),
		// Anti-regression: we must not pattern-match on the message.
		Entry("'not found' text without a gRPC status is Transient",
			errors.New("the wal file is not found"), restoreErrorTransient),
	)
})

var _ = Describe("classifyBarmanError", func() {
	DescribeTable("delegates to ErrWALNotFound sentinel",
		func(err error, expected restoreErrorKind) {
			Expect(classifyBarmanError(err)).To(Equal(expected))
		},
		Entry("nil is None", nil, restoreErrorNone),
		Entry("ErrWALNotFound (wrapped or bare) is NotFound",
			fmt.Errorf("ctx: %w", barmanRestorer.ErrWALNotFound), restoreErrorNotFound),
		Entry("any other error is Transient",
			errors.New("connectivity issue"), restoreErrorTransient),
	)
})

var _ = Describe("isTransientRestoreError", func() {
	DescribeTable("classifies errors",
		func(err error, expected bool) {
			Expect(isTransientRestoreError(err)).To(Equal(expected))
		},
		Entry("nil is not transient", nil, false),
		Entry("ErrWALNotFound is not transient", barmanRestorer.ErrWALNotFound, false),
		Entry("wrapped ErrWALNotFound is not transient",
			fmt.Errorf("ctx: %w", barmanRestorer.ErrWALNotFound), false),
		Entry("ErrNoBackupConfigured is not transient", ErrNoBackupConfigured, false),
		Entry("ErrEndOfWALStreamReached is not transient", ErrEndOfWALStreamReached, false),
		Entry("ErrExternalClusterNotFound is not transient", ErrExternalClusterNotFound, false),
		Entry("ErrRetryTimeoutReached is not transient (would loop forever otherwise)",
			ErrRetryTimeoutReached, false),
		Entry("a generic error is transient", errors.New("oops"), true),
	)
})

var _ = Describe("resolveMaxRetryTimeout", func() {
	clusterWith := func(d *metav1.Duration) *apiv1.Cluster {
		return &apiv1.Cluster{Spec: apiv1.ClusterSpec{WalRestoreRetryTimeout: d}}
	}

	DescribeTable("picks the retry budget",
		func(cluster *apiv1.Cluster, expected time.Duration) {
			Expect(resolveMaxRetryTimeout(cluster)).To(Equal(expected))
		},
		Entry("nil cluster → default", nil, DefaultMaxRetryTimeout),
		Entry("unset field → default", clusterWith(nil), DefaultMaxRetryTimeout),
		Entry("positive duration is honored",
			clusterWith(&metav1.Duration{Duration: 42 * time.Second}), 42*time.Second),
		// Zero and negative both hit the defense-in-depth branch; one
		// entry per polarity is enough to catch regressions in that guard.
		Entry("zero → default (defense in depth — webhook rejects this)",
			clusterWith(&metav1.Duration{Duration: 0}), DefaultMaxRetryTimeout),
		Entry("negative → default (defense in depth)",
			clusterWith(&metav1.Duration{Duration: -5 * time.Minute}), DefaultMaxRetryTimeout),
	)
})

var _ = Describe("nextBackoff", func() {
	DescribeTable("doubles each step up to the cap",
		func(in, expected time.Duration) {
			Expect(nextBackoff(in)).To(Equal(expected))
		},
		Entry("zero bootstraps to the initial value", time.Duration(0), retryBackoffInitial),
		Entry("negative bootstraps to the initial value", -1*time.Second, retryBackoffInitial),
		Entry("doubles when below the cap", 4*time.Second, 8*time.Second),
		Entry("saturates at the cap", retryBackoffCap, retryBackoffCap),
		Entry("does not overflow past the cap", 2*retryBackoffCap, retryBackoffCap),
	)
})

var _ = Describe("resolveNoBarmanError", func() {
	cluster := &apiv1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: "c"}}

	It("wraps config errors that aren't ErrNoBackupConfigured", func() {
		original := errors.New("kapow")
		err := resolveNoBarmanError(context.Background(), cluster, "wal", nil, restoreErrorNone, original)
		Expect(errors.Is(err, original)).To(BeTrue())
		Expect(errors.Is(err, ErrNoBackupConfigured)).To(BeFalse())
	})

	It("propagates ErrNoBackupConfigured when no plugin attempt was transient", func() {
		// Covers both "no plugin tried" (None) and "plugin said NotFound":
		// in either case PostgreSQL should see exit 1 and move on to
		// streaming replication.
		for _, kind := range []restoreErrorKind{restoreErrorNone, restoreErrorNotFound} {
			err := resolveNoBarmanError(context.Background(), cluster, "wal",
				nil, kind, ErrNoBackupConfigured)
			Expect(errors.Is(err, ErrNoBackupConfigured)).To(BeTrue(),
				"kind=%v should propagate", kind)
		}
	})

	It("hides ErrNoBackupConfigured when the plugin had a transient error", func() {
		// The bug fix: a transient plugin error must not be silently
		// downgraded to "no backup configured" → exit 1, which would let
		// PostgreSQL promote on a partial archive.
		pluginErr := status.Error(codes.Unavailable, "dial tcp")
		err := resolveNoBarmanError(context.Background(), cluster, "wal",
			pluginErr, restoreErrorTransient, ErrNoBackupConfigured)
		Expect(errors.Is(err, ErrNoBackupConfigured)).To(BeFalse())
		Expect(errors.Is(err, pluginErr)).To(BeTrue())
		Expect(isTransientRestoreError(err)).To(BeTrue())
	})
})

var _ = Describe("combineBarmanFailureWithPluginContext", func() {
	It("returns the barman error untouched when the plugin was not transient", func() {
		err := combineBarmanFailureWithPluginContext(nil, restoreErrorNone, barmanRestorer.ErrWALNotFound)
		Expect(errors.Is(err, barmanRestorer.ErrWALNotFound)).To(BeTrue())
	})

	It("returns the barman transient error untouched even when the plugin was transient", func() {
		// Both transient → just return barman's error; the retry loop
		// will classify it via the default branch in isTransientRestoreError.
		barmanErr := errors.New("connectivity")
		err := combineBarmanFailureWithPluginContext(
			errors.New("plugin flaked"), restoreErrorTransient, barmanErr)
		Expect(err).To(Equal(barmanErr))
		Expect(isTransientRestoreError(err)).To(BeTrue())
	})

	It("breaks the NotFound chain when the plugin was transient", func() {
		// The subtle contract: barman's NotFound must NOT be
		// errors.Is-detectable as ErrWALNotFound, otherwise the loop
		// treats it as final and skips retrying the (possibly recoverable)
		// plugin path.
		pluginErr := status.Error(codes.Unavailable, "blip")
		err := combineBarmanFailureWithPluginContext(pluginErr, restoreErrorTransient, barmanRestorer.ErrWALNotFound)
		Expect(errors.Is(err, barmanRestorer.ErrWALNotFound)).To(BeFalse())
		Expect(isTransientRestoreError(err)).To(BeTrue())
	})
})

var _ = Describe("retryUntilDeadline", func() {
	// Shrink the backoff knobs for the duration of these tests so the
	// loop runs in milliseconds. The specific production schedule is
	// already covered by nextBackoff's own test.
	var (
		origInitial time.Duration
		origCap     time.Duration
	)

	BeforeEach(func() {
		origInitial = retryBackoffInitial
		origCap = retryBackoffCap
		retryBackoffInitial = 2 * time.Millisecond
		retryBackoffCap = 10 * time.Millisecond
	})

	AfterEach(func() {
		retryBackoffInitial = origInitial
		retryBackoffCap = origCap
	})

	It("returns success after exactly one attempt", func() {
		var calls int32
		err := retryUntilDeadline(
			context.Background(),
			func(_ context.Context) error {
				atomic.AddInt32(&calls, 1)
				return nil
			},
			time.Now().Add(1*time.Second),
		)
		Expect(err).ToNot(HaveOccurred())
		Expect(atomic.LoadInt32(&calls)).To(Equal(int32(1)))
	})

	It("returns NotFound immediately without retrying", func() {
		var calls int32
		err := retryUntilDeadline(
			context.Background(),
			func(_ context.Context) error {
				atomic.AddInt32(&calls, 1)
				return barmanRestorer.ErrWALNotFound
			},
			time.Now().Add(1*time.Second),
		)
		Expect(errors.Is(err, barmanRestorer.ErrWALNotFound)).To(BeTrue())
		Expect(atomic.LoadInt32(&calls)).To(Equal(int32(1)))
	})

	It("retries transient errors until a success is returned", func() {
		var calls int32
		err := retryUntilDeadline(
			context.Background(),
			func(_ context.Context) error {
				if atomic.AddInt32(&calls, 1) < 3 {
					return errors.New("transient blip")
				}
				return nil
			},
			time.Now().Add(1*time.Second),
		)
		Expect(err).ToNot(HaveOccurred())
		Expect(atomic.LoadInt32(&calls)).To(Equal(int32(3)))
	})

	It("surfaces ErrRetryTimeoutReached wrapping the last transient error when the deadline is hit", func() {
		lastErr := errors.New("flaky bucket")
		err := retryUntilDeadline(
			context.Background(),
			func(_ context.Context) error { return lastErr },
			time.Now().Add(20*time.Millisecond),
		)
		Expect(errors.Is(err, ErrRetryTimeoutReached)).To(BeTrue())
		Expect(errors.Is(err, lastErr)).To(BeTrue())
	})

	It("honors a NotFound outcome that arrives mid-retry (no more retries after it)", func() {
		var calls int32
		err := retryUntilDeadline(
			context.Background(),
			func(_ context.Context) error {
				if atomic.AddInt32(&calls, 1) == 1 {
					return errors.New("one transient")
				}
				return barmanRestorer.ErrWALNotFound
			},
			time.Now().Add(1*time.Second),
		)
		Expect(errors.Is(err, barmanRestorer.ErrWALNotFound)).To(BeTrue())
		Expect(atomic.LoadInt32(&calls)).To(Equal(int32(2)))
	})

	It("maps context cancellation to ErrRetryTimeoutReached (don't let PostgreSQL promote)", func() {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err := retryUntilDeadline(
			ctx,
			func(_ context.Context) error { return errors.New("transient") },
			time.Now().Add(1*time.Second),
		)
		Expect(errors.Is(err, ErrRetryTimeoutReached)).To(BeTrue())
		Expect(errors.Is(err, context.Canceled)).To(BeTrue())
	})

	It("always makes at least one attempt, even with an already-past deadline", func() {
		var calls int32
		err := retryUntilDeadline(
			context.Background(),
			func(_ context.Context) error {
				atomic.AddInt32(&calls, 1)
				return errors.New("transient")
			},
			time.Now().Add(-1*time.Second),
		)
		Expect(errors.Is(err, ErrRetryTimeoutReached)).To(BeTrue())
		Expect(atomic.LoadInt32(&calls)).To(Equal(int32(1)))
	})
})
