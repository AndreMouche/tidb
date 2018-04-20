// Copyright 2018 PingCAP, Inc.
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

package latch

// LatchesScheduler is used to scheduler latches
type LatchesScheduler struct {
	latches *Latches
	msgCh   chan *Lock
}

//NewLatchesScheduler create the LatchesScheduler.
func NewLatchesScheduler(size int) *LatchesScheduler {
	latches := NewLatches(size)
	msgCh := make(chan *Lock, 100)
	scheduler := &LatchesScheduler{
		latches: latches,
		msgCh:   msgCh,
	}
	go scheduler.run()
	return scheduler
}

func (scheduler *LatchesScheduler) run() {
	for lock := range scheduler.msgCh {
		wakeupList := scheduler.latches.Release(lock, lock.commitTS)
		if len(wakeupList) > 0 {
			scheduler.wakeup(wakeupList)
		}
	}
}

func (scheduler *LatchesScheduler) wakeup(wakeupList []*Lock) {
	for _, lock := range wakeupList {
		if scheduler.latches.Acquire(lock) != AcquireLocked {
			lock.wg.Done()
		}
	}
}

// Close close LatchesScheduler
func (scheduler *LatchesScheduler) Close() {
	close(scheduler.msgCh)
}

// Lock acquire the lock for transaction with startTS and keys
func (scheduler *LatchesScheduler) Lock(startTS uint64, keys [][]byte) *Lock {
	lock := scheduler.latches.GenLock(startTS, keys)
	lock.wg.Add(1)
	if scheduler.latches.Acquire(lock) == AcquireLocked {
		lock.wg.Wait()
	}
	if lock.status == AcquireLocked {
		panic("should never run here")
	}
	return lock
}

// UnLock unlocks a lock with commitTS
func (scheduler *LatchesScheduler) UnLock(lock *Lock, commitTS uint64) {
	lock.commitTS = commitTS
	scheduler.msgCh <- lock
}

// RefreshCommitTS refresh commitTS for keys.
func (scheduler *LatchesScheduler) RefreshCommitTS(keys [][]byte, commitTS uint64) {
	scheduler.latches.refreshCommitTS(keys, commitTS)
}
