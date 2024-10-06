//go:build !production
// +build !production

// SPDX-License-Identifier: Apache-2.0
// Copyright Authors of Cilium

package kvstore

import (
	"context"
	"testing"

	"github.com/cilium/cilium/pkg/inctimer"
	"github.com/cilium/cilium/pkg/time"
)

// SetupDummy sets up kvstore for tests.
func SetupDummy(tb testing.TB, dummyBackend string) {
	SetupDummyWithConfigOpts(tb, dummyBackend, nil)
}

// SetupDummyWithConfigOpts sets up the dummy kvstore for tests but also
// configures the module with the provided opts.
func SetupDummyWithConfigOpts(tb testing.TB, dummyBackend string, opts map[string]string) {
	module := getBackend(dummyBackend)
	if module == nil {
		tb.Fatalf("Unknown dummy kvstore backend %s", dummyBackend)
	}

	module.setConfigDummy()

	if opts != nil {
		err := module.setConfig(opts)
		if err != nil {
			tb.Fatalf("Unable to set config options for kvstore backend module: %v", err)
		}
	}

	if err := initClient(context.Background(), module, nil); err != nil {
		tb.Fatalf("Unable to initialize kvstore client: %v", err)
	}

	tb.Cleanup(func() {
		if err := Client().DeletePrefix(context.Background(), ""); err != nil {
			tb.Fatalf("Unable to delete all kvstore keys: %v", err)
		}
		Client().Close()
	})

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	if err := <-Client().Connected(ctx); err != nil {
		tb.Fatalf("Failed waiting for kvstore connection to be established: %v", err)
	}

	timer, done := inctimer.New()
	defer done()

	// Implement a locking mechanism to ensure that only one test at a time can
	// access the kvstore, preventing interaction between tests.
	for {
		succeeded, err := Client().CreateOnly(ctx, ".lock", []byte(""), true)
		if err != nil {
			tb.Fatalf("Unable to acquire the kvstore lock: %v", err)
		}

		if succeeded {
			return
		}

		select {
		case <-timer.After(100 * time.Millisecond):
		case <-ctx.Done():
			tb.Fatal("Timed out waiting to acquire the kvstore lock")
		}
	}
}
