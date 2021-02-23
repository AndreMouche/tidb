// Copyright 2020 PingCAP, Inc.
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

package mockstore

import (
	"context"
	"crypto/tls"
	"fmt"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/mockstore/unistore"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/store/tikv/logutil"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/execdetails"
	"github.com/pingcap/tidb/util/rowcodec"
	"go.uber.org/zap"
)

func newUnistore(opts *mockOptions) (kv.Storage, error) {
	client, pdClient, cluster, err := unistore.New(opts.path)
	if err != nil {
		return nil, errors.Trace(err)
	}
	opts.clusterInspector(cluster)
	pdClient = execdetails.InterceptedPDClient{
		Client: pdClient,
	}

	kvstore, err := tikv.NewTestTiKVStore(client, pdClient, opts.clientHijacker, opts.pdClientHijacker, opts.txnLocalLatches)
	if err != nil {
		return nil, err
	}
	return &mockStorage{KVStore: kvstore}, nil
}

// Wraps tikv.KVStore and make it compatible with kv.Storage.
type mockStorage struct {
	*tikv.KVStore
}

func (s *mockStorage) EtcdAddrs() ([]string, error) {
	return nil, nil
}

func (s *mockStorage) TLSConfig() *tls.Config {
	return nil
}

func (s *mockStorage) StartGCWorker() error {
	return nil
}

// Close and unregister the store.
func (s *mockStorage) Close() error {
	return s.KVStore.Close()
}

// Begin a global transaction.
func (s *mockStorage) Begin() (kv.Transaction, error) {
	return s.BeginWithTxnScope(oracle.GlobalTxnScope)
}

func (s *mockStorage) BeginWithTxnScope(txnScope string) (kv.Transaction, error) {
	txn, err := s.KVStore.BeginWithTxnScope(txnScope)
	if err != nil {
		return txn, errors.Trace(err)
	}
	return NewTiKVTxn(txn.(*tikv.TikvTxn)), err
}

// BeginWithStartTS begins a transaction with startTS.
func (s *mockStorage) BeginWithStartTS(txnScope string, startTS uint64) (kv.Transaction, error) {
	txn, err := s.KVStore.BeginWithStartTS(txnScope, startTS)
	if err != nil {
		return txn, errors.Trace(err)
	}
	return NewTiKVTxn(txn.(*tikv.TikvTxn)), err
}

// BeginWithExactStaleness begins transaction with given staleness
func (s *mockStorage) BeginWithExactStaleness(txnScope string, prevSec uint64) (kv.Transaction, error) {
	txn, err := s.KVStore.BeginWithExactStaleness(txnScope, prevSec)
	if err != nil {
		return txn, errors.Trace(err)
	}
	return NewTiKVTxn(txn.(*tikv.TikvTxn)), err
}

type tikvTxn struct {
	*tikv.TikvTxn
	idxNameCache map[int64]*model.TableInfo
}

func NewTiKVTxn(txn *tikv.TikvTxn) *tikvTxn {
	return &tikvTxn{txn, make(map[int64]*model.TableInfo)}
}

func (txn *tikvTxn) GetTableInfo(id int64) *model.TableInfo {
	return txn.idxNameCache[id]
}

func (txn *tikvTxn) CacheTableInfo(id int64, info *model.TableInfo) {
	txn.idxNameCache[id] = info
}

// lockWaitTime in ms, except that kv.LockAlwaysWait(0) means always wait lock, kv.LockNowait(-1) means nowait lock
func (txn *tikvTxn) LockKeys(ctx context.Context, lockCtx *kv.LockCtx, keysInput ...kv.Key) error {
	err := txn.TikvTxn.LockKeys(ctx, lockCtx, keysInput...)
	if e, ok := err.(*tikv.ErrKeyExist); ok {
		return txn.extractKeyExistsErr(e.Key())
	}
	return errors.Trace(err)
}

func (txn tikvTxn) Commit(ctx context.Context) error {
	err := txn.TikvTxn.Commit(ctx)
	if e, ok := err.(*tikv.ErrKeyExist); ok {
		return txn.extractKeyExistsErr(e.Key())
	}
	return errors.Trace(err)
}

func (txn *tikvTxn) extractKeyExistsErr(key kv.Key) error {
	if !txn.GetUnionStore().HasPresumeKeyNotExists(key) {
		return errors.Errorf("session %d, existErr for key:%s should not be nil", txn.SessionID(), key)
	}

	tableID, indexID, isRecord, err := tablecodec.DecodeKeyHead(key)
	if err != nil {
		return genKeyExistsError("UNKNOWN", key.String(), err)
	}

	tblInfo := txn.GetTableInfo(tableID)
	if tblInfo == nil {
		return genKeyExistsError("UNKNOWN", key.String(), errors.New("cannot find table info"))
	}

	value, err := txn.GetUnionStore().GetMemBuffer().SelectValueHistory(key, func(value []byte) bool { return len(value) != 0 })
	if err != nil {
		return genKeyExistsError("UNKNOWN", key.String(), err)
	}

	if isRecord {
		return extractKeyExistsErrFromHandle(key, value, tblInfo)
	}
	return extractKeyExistsErrFromIndex(key, value, tblInfo, indexID)
}

func genKeyExistsError(name string, value string, err error) error {
	if err != nil {
		logutil.BgLogger().Info("extractKeyExistsErr meets error", zap.Error(err))
	}
	return kv.ErrKeyExists.FastGenByArgs(value, name)
}

func extractKeyExistsErrFromHandle(key kv.Key, value []byte, tblInfo *model.TableInfo) error {
	const name = "PRIMARY"
	_, handle, err := tablecodec.DecodeRecordKey(key)
	if err != nil {
		return genKeyExistsError(name, key.String(), err)
	}

	if handle.IsInt() {
		if pkInfo := tblInfo.GetPkColInfo(); pkInfo != nil {
			if mysql.HasUnsignedFlag(pkInfo.Flag) {
				handleStr := fmt.Sprintf("%d", uint64(handle.IntValue()))
				return genKeyExistsError(name, handleStr, nil)
			}
		}
		return genKeyExistsError(name, handle.String(), nil)
	}

	if len(value) == 0 {
		return genKeyExistsError(name, handle.String(), errors.New("missing value"))
	}

	idxInfo := tables.FindPrimaryIndex(tblInfo)
	if idxInfo == nil {
		return genKeyExistsError(name, handle.String(), errors.New("cannot find index info"))
	}

	cols := make(map[int64]*types.FieldType, len(tblInfo.Columns))
	for _, col := range tblInfo.Columns {
		cols[col.ID] = &col.FieldType
	}
	handleColIDs := make([]int64, 0, len(idxInfo.Columns))
	for _, col := range idxInfo.Columns {
		handleColIDs = append(handleColIDs, tblInfo.Columns[col.Offset].ID)
	}

	row, err := tablecodec.DecodeRowToDatumMap(value, cols, time.Local)
	if err != nil {
		return genKeyExistsError(name, handle.String(), err)
	}

	data, err := tablecodec.DecodeHandleToDatumMap(handle, handleColIDs, cols, time.Local, row)
	if err != nil {
		return genKeyExistsError(name, handle.String(), err)
	}

	valueStr := make([]string, 0, len(data))
	for _, col := range idxInfo.Columns {
		d := data[tblInfo.Columns[col.Offset].ID]
		str, err := d.ToString()
		if err != nil {
			return genKeyExistsError(name, key.String(), err)
		}
		valueStr = append(valueStr, str)
	}
	return genKeyExistsError(name, strings.Join(valueStr, "-"), nil)
}

func extractKeyExistsErrFromIndex(key kv.Key, value []byte, tblInfo *model.TableInfo, indexID int64) error {
	var idxInfo *model.IndexInfo
	for _, index := range tblInfo.Indices {
		if index.ID == indexID {
			idxInfo = index
		}
	}
	if idxInfo == nil {
		return genKeyExistsError("UNKNOWN", key.String(), errors.New("cannot find index info"))
	}
	name := idxInfo.Name.String()

	if len(value) == 0 {
		return genKeyExistsError(name, key.String(), errors.New("missing value"))
	}

	colInfo := make([]rowcodec.ColInfo, 0, len(idxInfo.Columns))
	for _, idxCol := range idxInfo.Columns {
		col := tblInfo.Columns[idxCol.Offset]
		colInfo = append(colInfo, rowcodec.ColInfo{
			ID:         col.ID,
			IsPKHandle: tblInfo.PKIsHandle && mysql.HasPriKeyFlag(col.Flag),
			Ft:         rowcodec.FieldTypeFromModelColumn(col),
		})
	}

	values, err := tablecodec.DecodeIndexKV(key, value, len(idxInfo.Columns), tablecodec.HandleNotNeeded, colInfo)
	if err != nil {
		return genKeyExistsError(name, key.String(), err)
	}
	valueStr := make([]string, 0, len(values))
	for i, val := range values {
		d, err := tablecodec.DecodeColumnValue(val, colInfo[i].Ft, time.Local)
		if err != nil {
			return genKeyExistsError(name, key.String(), err)
		}
		str, err := d.ToString()
		if err != nil {
			return genKeyExistsError(name, key.String(), err)
		}
		valueStr = append(valueStr, str)
	}
	return genKeyExistsError(name, strings.Join(valueStr, "-"), nil)
}
