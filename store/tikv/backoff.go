// Copyright 2021 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package tikv

import (
	"context"

	"github.com/pingcap/tidb/store/tikv/kv"
	"github.com/pingcap/tidb/store/tikv/retry"
)

// Backoffer is a utility for retrying queries.
type Backoffer = retry.Backoffer

// BackoffType defines the backoff type.
type BackoffType = retry.BackoffType

// Back off types.
const (
	BoRegionMiss  = retry.BoRegionMiss
	BoTiKVRPC     = retry.BoTiKVRPC
	BoTiFlashRPC  = retry.BoTiFlashRPC
	BoTxnLockFast = retry.BoTxnLockFast
	BoTxnLock     = retry.BoTxnLock
	BoPDRPC       = retry.BoPDRPC
)

// Maximum total sleep time(in ms) for kv/cop commands.
const (
	gcResolveLockMaxBackoff = 100000
)

var (
	// CommitMaxBackoff is max sleep time of the 'commit' command
	CommitMaxBackoff = uint64(41000)
	// PrewriteMaxBackoff is max sleep time of the `pre-write` command.
	PrewriteMaxBackoff = 20000
)

// NewBackofferWithVars creates a Backoffer with maximum sleep time(in ms) and kv.Variables.
func NewBackofferWithVars(ctx context.Context, maxSleep int, vars *kv.Variables) *Backoffer {
	return retry.NewBackofferWithVars(ctx, maxSleep, vars)
}

// NewBackoffer creates a Backoffer with maximum sleep time(in ms).
func NewBackoffer(ctx context.Context, maxSleep int) *Backoffer {
	return retry.NewBackoffer(ctx, maxSleep)
}

// TxnStartKey is a key for transaction start_ts info in context.Context.
func TxnStartKey() interface{} {
	return retry.TxnStartKey
}

// NewGcResolveLockMaxBackoffer creates a Backoffer for Gc to resolve lock.
func NewGcResolveLockMaxBackoffer(ctx context.Context) *Backoffer {
	return retry.NewBackofferWithVars(ctx, gcResolveLockMaxBackoff, nil)
}
