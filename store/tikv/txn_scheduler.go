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

package tikv

import (
	"fmt"
	"sync"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/store/tikv/latch"
	binlog "github.com/pingcap/tipb/go-binlog"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
)

type txnCommiter struct {
	commiter *twoPhaseCommitter
	ch       chan error
	ctx      context.Context
	lock     latch.Lock
}

func (txn *txnCommiter) execute(timeout bool) (commitTS uint64) {
	commitTS = uint64(0)
	if timeout {
		err := errors.Errorf("startTS %d timeout", txn.commiter.startTS)
		log.Warn(txn.commiter.connID, err)
		txn.ch <- errors.Annotate(err, txnRetryableMark)
		return
	}

	err := txn.commiter.execute(txn.ctx)

	if err != nil {
		txn.commiter.writeFinishBinlog(binlog.BinlogType_Rollback, 0)
	} else {
		commitTS = txn.commiter.commitTS
		txn.commiter.writeFinishBinlog(binlog.BinlogType_Commit, int64(commitTS))
	}
	txn.ch <- errors.Trace(err)
	log.Debug(txn.commiter.connID, "finish txn with startTS:", txn.commiter.startTS, " commitTS:", commitTS, " error:", err)
	return commitTS
}

type txnMap struct {
	txns map[uint64]*txnCommiter
	sync.RWMutex
}

func (t *txnMap) put(startTS uint64, txn *txnCommiter) {
	t.Lock()
	defer t.Unlock()
	if t.txns == nil {
		t.txns = make(map[uint64]*txnCommiter)
	}
	if _, ok := t.txns[startTS]; ok {
		panic(fmt.Sprintf("The startTS %d shouldn't be used in two transaction", txn.commiter.startTS))
	}
	t.txns[startTS] = txn
}

func (t *txnMap) get(startTS uint64) *txnCommiter {
	t.RLock()
	defer t.RUnlock()
	return t.txns[startTS]
}

func (t *txnMap) delete(startTS uint64) {
	t.Lock()
	defer t.Unlock()
	delete(t.txns, startTS)
}

type txnScheduler struct {
	latches latch.Latches
	txns    []txnMap
}

func newTxnScheduler() txnScheduler {
	return txnScheduler{
		latches: latch.NewLatches(1024000), //TODO
		txns:    make([]txnMap, 256, 256),
	}
}

func (store *txnScheduler) execute(ctx context.Context, txn *tikvTxn, connID uint64) (err error) {
	commiter, err := newTwoPhaseCommitter(txn, connID)
	if err != nil || commiter == nil {
		return errors.Trace(err)
	}
	ch := make(chan error, 1)
	lock := store.latches.GenLock(commiter.startTS, commiter.keys)
	store.getMap(txn.startTS).put(txn.startTS, &txnCommiter{
		commiter,
		ch,
		ctx,
		lock,
	})

	go store.run(commiter.startTS)
	err = errors.Trace(<-ch)
	if err == nil {
		txn.commitTS = commiter.commitTS
	}
	return err
}

func (store *txnScheduler) getMap(startTS uint64) *txnMap {
	id := int(startTS % uint64(len(store.txns)))
	return &store.txns[id]
}

func (store *txnScheduler) run(startTS uint64) {
	txn := store.getMap(startTS).get(startTS)
	if txn == nil {
		panic(startTS)
	}
	acquired, timeout := store.latches.Acquire(&txn.lock)
	if !timeout && !acquired {
		// wait for next wakeup
		return
	}
	commitTS := txn.execute(timeout)
	wakeupList := store.latches.Release(&txn.lock, commitTS)
	for _, s := range wakeupList {
		go store.run(s)
	}
	store.getMap(startTS).delete(startTS)
}
