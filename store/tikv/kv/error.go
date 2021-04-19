// Copyright 2016 PingCAP, Inc.
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

package kv

import (
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	mysql "github.com/pingcap/tidb/store/tikv/errno"
	"github.com/pingcap/tidb/util/dbterror"
)

var (
	// ErrBodyMissing response body is missing error
	ErrBodyMissing = errors.New("response body is missing")
	// ErrTiDBShuttingDown is returned when TiDB is closing and send request to tikv fail, do not retry.
	ErrTiDBShuttingDown = errors.New("tidb server shutting down")
)

// MismatchClusterID represents the message that the cluster ID of the PD client does not match the PD.
const MismatchClusterID = "mismatch cluster id"

// MySQL error instances.
var (
	ErrTiKVServerTimeout           = dbterror.ClassTiKV.NewStd(mysql.ErrTiKVServerTimeout)
	ErrTiFlashServerTimeout        = dbterror.ClassTiKV.NewStd(mysql.ErrTiFlashServerTimeout)
	ErrResolveLockTimeout          = dbterror.ClassTiKV.NewStd(mysql.ErrResolveLockTimeout)
	ErrPDServerTimeout             = dbterror.ClassTiKV.NewStd(mysql.ErrPDServerTimeout)
	ErrRegionUnavailable           = dbterror.ClassTiKV.NewStd(mysql.ErrRegionUnavailable)
	ErrTiKVServerBusy              = dbterror.ClassTiKV.NewStd(mysql.ErrTiKVServerBusy)
	ErrTiFlashServerBusy           = dbterror.ClassTiKV.NewStd(mysql.ErrTiFlashServerBusy)
	ErrTiKVStaleCommand            = dbterror.ClassTiKV.NewStd(mysql.ErrTiKVStaleCommand)
	ErrTiKVMaxTimestampNotSynced   = dbterror.ClassTiKV.NewStd(mysql.ErrTiKVMaxTimestampNotSynced)
	ErrGCTooEarly                  = dbterror.ClassTiKV.NewStd(mysql.ErrGCTooEarly)
	ErrQueryInterrupted            = dbterror.ClassTiKV.NewStd(mysql.ErrQueryInterrupted)
	ErrLockAcquireFailAndNoWaitSet = dbterror.ClassTiKV.NewStd(mysql.ErrLockAcquireFailAndNoWaitSet)
	ErrLockWaitTimeout             = dbterror.ClassTiKV.NewStd(mysql.ErrLockWaitTimeout)
	ErrTokenLimit                  = dbterror.ClassTiKV.NewStd(mysql.ErrTiKVStoreLimit)
	ErrLockExpire                  = dbterror.ClassTiKV.NewStd(mysql.ErrLockExpire)
	ErrUnknown                     = dbterror.ClassTiKV.NewStd(mysql.ErrUnknown)

	ErrNotExist = NewError(mysql.ErrCodeNotExist)
)

// Registers error returned from TiKV.
var (
	_ = dbterror.ClassTiKV.NewStd(mysql.ErrDataOutOfRange)
	_ = dbterror.ClassTiKV.NewStd(mysql.ErrTruncatedWrongValue)
	_ = dbterror.ClassTiKV.NewStd(mysql.ErrDivisionByZero)
)

//TiKVError is a normal error in tikv.
type TiKVError struct {
	code int
	msg  string
}

// NewError creates a TiKVError
func NewError(code int) *TiKVError {
	return &TiKVError{
		code: code,
		msg:  "",
	}
}

// Error implements the error interface.
func (e *TiKVError) Error() string {
	return fmt.Sprintf("code:%d,msg:%s", e.code, e.msg)
}

// Equal checks whether the errors are equal.
func (e *TiKVError) Equal(other error) bool {
	if o, ok := errors.Cause(other).(*TiKVError); ok {
		return o.Code() == e.Code()
	}
	return false
}

// Code returns error code.
func (e *TiKVError) Code() int {
	return e.code
}

// IsErrNotFound checks if err is a kind of NotFound error.
func IsErrNotFound(err error) bool {
	return ErrNotExist.Equal(err)
}

// ErrDeadlock wraps *kvrpcpb.Deadlock to implement the error interface.
// It also marks if the deadlock is retryable.
type ErrDeadlock struct {
	*kvrpcpb.Deadlock
	IsRetryable bool
}

func (d *ErrDeadlock) Error() string {
	return d.Deadlock.String()
}

// PDError wraps *pdpb.Error to implement the error interface.
type PDError struct {
	Err *pdpb.Error
}

func (d *PDError) Error() string {
	return d.Err.String()
}

// ErrKeyExist wraps *pdpb.AlreadyExist to implement the error interface.
type ErrKeyExist struct {
	*kvrpcpb.AlreadyExist
}

func (k *ErrKeyExist) Error() string {
	return k.AlreadyExist.String()
}

// IsErrKeyExist returns true if it is ErrKeyExist.
func IsErrKeyExist(err error) bool {
	_, ok := errors.Cause(err).(*ErrKeyExist)
	return ok
}

// ErrWriteConflict wraps *kvrpcpb.ErrWriteConflict to implement the error interface.
type ErrWriteConflict struct {
	*kvrpcpb.WriteConflict
}

func (k *ErrWriteConflict) Error() string {
	return k.WriteConflict.String()
}

// IsErrWriteConflict returns true if it is ErrWriteConflict.
func IsErrWriteConflict(err error) bool {
	_, ok := errors.Cause(err).(*ErrWriteConflict)
	return ok
}

//NewErrWriteConfictWithArgs generates an ErrWriteConflict with args.
func NewErrWriteConfictWithArgs(startTs, conflictTs, conflictCommitTs uint64, key []byte) *ErrWriteConflict {
	conflict := kvrpcpb.WriteConflict{
		StartTs:          startTs,
		ConflictTs:       conflictTs,
		Key:              key,
		ConflictCommitTs: conflictCommitTs,
	}
	return &ErrWriteConflict{WriteConflict: &conflict}
}

// ErrRetryable wraps *kvrpcpb.Retryable to implement the error interface.
type ErrRetryable struct {
	Retryable string
}

func (k *ErrRetryable) Error() string {
	return k.Retryable
}
