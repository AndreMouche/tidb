package tikv

import (
	"github.com/juju/errors"
	"github.com/pingcap/tidb/store/tikv/latch"
	binlog "github.com/pingcap/tipb/go-binlog"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"sync"
)

type txnCommiter struct {
	commiter *twoPhaseCommitter
	ch       chan error
	ctx      context.Context
	commitTs uint64
	lock latch.Lock
}

func (txn *txnCommiter) StartTs()uint64{
	return txn.commiter.startTS
}

func (txn *txnCommiter) CommitTs()uint64 {
	return txn.commitTs
}

func (txn *txnCommiter) GetLock()*latch.Lock{
	return &txn.lock
}
func (txn *txnCommiter) execute(timeout bool) {
	if timeout {
		err := errors.Errorf("startTs %d timeout", txn.commiter.startTS)
		log.Warn(txn.commiter.connID, err)
		txn.ch <- errors.Annotate(err, txnRetryableMark)
		return
	}

	err := txn.commiter.execute(txn.ctx)

	if err != nil {
		txn.commiter.writeFinishBinlog(binlog.BinlogType_Rollback, 0)
	} else {
		txn.commitTs = txn.commiter.commitTS
		txn.commiter.writeFinishBinlog(binlog.BinlogType_Commit, int64(txn.commitTs))
	}
	txn.ch <- errors.Trace(err)
	log.Debug(txn.commiter.connID, "finish txn with startTs:", txn.commiter.startTS, " commitTs:", txn.commitTs, " error:", err)
	return
}

type txnStore struct {
	latches latch.Latches
	sync.RWMutex
}

func newTxnStore() txnStore {
	return txnStore{
		latch.NewLatches(1024000), //TODO
		sync.RWMutex{},
	}
}

func (store *txnStore) execute(ctx context.Context, txn *tikvTxn, connID uint64) (err error) {
	commiter, err := newTwoPhaseCommitter(txn, connID)
	if err != nil || commiter == nil {
		return errors.Trace(err)
	}
	ch := make(chan error)
	lock := store.latches.GenLock(commiter.keys)
	t := &txnCommiter{
		commiter,
		ch,
		ctx,
		0,
		lock,
	}

	go store.run(t)
	err = errors.Trace(<-ch)
	if err == nil {
		txn.commitTS = commiter.commitTS
	}
	return err
}

func (store *txnStore) run(txn *txnCommiter) {
	acquired, timeout := store.latches.Acquire(txn)
	if !timeout && !acquired {
		// wait for next wakeup
		return
	}
	txn.execute(timeout)
	wakeupList := store.latches.Release(txn)
	for _, s := range wakeupList {
		t := s.(*txnCommiter)
		go store.run(t)
	}
}
