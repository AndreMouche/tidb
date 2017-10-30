// Copyright 2017 PingCAP, Inc.
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

package tikvrpc

import (
	"fmt"
	"io"

	log "github.com/Sirupsen/logrus"
	"github.com/golang/protobuf/proto"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/tikvpb"
	"github.com/pingcap/tipb/go-tipb"
)

// CmdType represents the concrete request type in Request or response type in Response.
type CmdType uint16

// CmdType values.
const (
	CmdGet CmdType = 1 + iota
	CmdScan
	CmdPrewrite
	CmdCommit
	CmdCleanup
	CmdBatchGet
	CmdBatchRollback
	CmdScanLock
	CmdResolveLock
	CmdGC
	CmdDeleteRange

	CmdRawGet CmdType = 256 + iota
	CmdRawPut
	CmdRawDelete
	CmdRawScan

	CmdCop CmdType = 512 + iota

	CmdMvccGetByKey CmdType = 1024 + iota
	CmdMvccGetByStartTs
	CmdSplitRegion
)

// CopResponse totot
type CopResponse struct {
	First    coprocessor.Response
	stream   tikvpb.Tikv_CoprocessorStreamClient
	id       int
	finished bool
}

// NewCopResponse totototot
func NewCopResponse(stream tikvpb.Tikv_CoprocessorStreamClient) (*CopResponse, error) {
	res := &CopResponse{
		stream:   stream,
		id:       0,
		finished: false,
	}
	first, err := res.Next()
	res.First = *first
	return res, err
}

func (self *CopResponse) Next() (*coprocessor.Response, error) {
	if self.finished {
		return nil, nil
	}
	self.id += 1
	s, err := self.stream.Recv()
	if err == io.EOF {
		//log.Warn("finished an stream", self.id)
		self.finished = true
		if self.id == 1 {
			b, err := proto.Marshal(&tipb.SelectResponse{})
			if err != nil {
				return nil, err
			}
			return &coprocessor.Response{Data: b}, nil
		}
		return nil, nil
	}
	if err != nil {
		log.Warn("meet error", err)
		self.finished = true
		return nil, err
	}

	//log.Warn("Get a new stream,", self.id)
	return s, err
}

//NewCopResponse...
func NewCopResponse2(stream tikvpb.Tikv_CoprocessorStreamClient) (*coprocessor.Response, error) {
	resp := &tipb.SelectResponse{}
	id := 0
	for {
		id += 1
		s, err := stream.Recv()
		if err == io.EOF {
			log.Warn("finished an stream", id)
			break
		}
		if err != nil {
			log.Warn(err)
			return nil, err
		}
		if s.GetLocked() != nil || s.GetOtherError() != "" || s.GetRegionError() != nil {
			return s, nil
		}
		sp := new(tipb.SelectResponse)
		err = proto.Unmarshal(s.Data, sp)
		if err != nil {
			return nil, err
		}
		log.Warn("append on stream lalala", id)
		resp.Chunks = append(resp.Chunks, sp.Chunks...)
	}

	b, err := proto.Marshal(resp)
	if err != nil {
		return nil, err
	}
	return &coprocessor.Response{Data: b}, nil
}

// Request wraps all kv/coprocessor requests.
type Request struct {
	kvrpcpb.Context
	Type             CmdType
	Get              *kvrpcpb.GetRequest
	Scan             *kvrpcpb.ScanRequest
	Prewrite         *kvrpcpb.PrewriteRequest
	Commit           *kvrpcpb.CommitRequest
	Cleanup          *kvrpcpb.CleanupRequest
	BatchGet         *kvrpcpb.BatchGetRequest
	BatchRollback    *kvrpcpb.BatchRollbackRequest
	ScanLock         *kvrpcpb.ScanLockRequest
	ResolveLock      *kvrpcpb.ResolveLockRequest
	GC               *kvrpcpb.GCRequest
	DeleteRange      *kvrpcpb.DeleteRangeRequest
	RawGet           *kvrpcpb.RawGetRequest
	RawPut           *kvrpcpb.RawPutRequest
	RawDelete        *kvrpcpb.RawDeleteRequest
	RawScan          *kvrpcpb.RawScanRequest
	Cop              *coprocessor.Request
	MvccGetByKey     *kvrpcpb.MvccGetByKeyRequest
	MvccGetByStartTs *kvrpcpb.MvccGetByStartTsRequest
	SplitRegion      *kvrpcpb.SplitRegionRequest
}

// Response wraps all kv/coprocessor responses.
type Response struct {
	Type             CmdType
	Get              *kvrpcpb.GetResponse
	Scan             *kvrpcpb.ScanResponse
	Prewrite         *kvrpcpb.PrewriteResponse
	Commit           *kvrpcpb.CommitResponse
	Cleanup          *kvrpcpb.CleanupResponse
	BatchGet         *kvrpcpb.BatchGetResponse
	BatchRollback    *kvrpcpb.BatchRollbackResponse
	ScanLock         *kvrpcpb.ScanLockResponse
	ResolveLock      *kvrpcpb.ResolveLockResponse
	GC               *kvrpcpb.GCResponse
	DeleteRange      *kvrpcpb.DeleteRangeResponse
	RawGet           *kvrpcpb.RawGetResponse
	RawPut           *kvrpcpb.RawPutResponse
	RawDelete        *kvrpcpb.RawDeleteResponse
	RawScan          *kvrpcpb.RawScanResponse
	Cop              *CopResponse
	MvccGetByKey     *kvrpcpb.MvccGetByKeyResponse
	MvccGetByStartTS *kvrpcpb.MvccGetByStartTsResponse
	SplitRegion      *kvrpcpb.SplitRegionResponse
}

// SetContext set the Context field for the given req to the specified ctx.
func SetContext(req *Request, region *metapb.Region, peer *metapb.Peer) error {
	ctx := &req.Context
	ctx.RegionId = region.Id
	ctx.RegionEpoch = region.RegionEpoch
	ctx.Peer = peer

	switch req.Type {
	case CmdGet:
		req.Get.Context = ctx
	case CmdScan:
		req.Scan.Context = ctx
	case CmdPrewrite:
		req.Prewrite.Context = ctx
	case CmdCommit:
		req.Commit.Context = ctx
	case CmdCleanup:
		req.Cleanup.Context = ctx
	case CmdBatchGet:
		req.BatchGet.Context = ctx
	case CmdBatchRollback:
		req.BatchRollback.Context = ctx
	case CmdScanLock:
		req.ScanLock.Context = ctx
	case CmdResolveLock:
		req.ResolveLock.Context = ctx
	case CmdGC:
		req.GC.Context = ctx
	case CmdDeleteRange:
		req.DeleteRange.Context = ctx
	case CmdRawGet:
		req.RawGet.Context = ctx
	case CmdRawPut:
		req.RawPut.Context = ctx
	case CmdRawDelete:
		req.RawDelete.Context = ctx
	case CmdRawScan:
		req.RawScan.Context = ctx
	case CmdCop:
		req.Cop.Context = ctx
	case CmdMvccGetByKey:
		req.MvccGetByKey.Context = ctx
	case CmdMvccGetByStartTs:
		req.MvccGetByStartTs.Context = ctx
	case CmdSplitRegion:
		req.SplitRegion.Context = ctx
	default:
		return fmt.Errorf("invalid request type %v", req.Type)
	}
	return nil
}

// GenRegionErrorResp returns corresponding Response with specified RegionError
// according to the given req.
func GenRegionErrorResp(req *Request, e *errorpb.Error) (*Response, error) {
	resp := &Response{}
	resp.Type = req.Type
	switch req.Type {
	case CmdGet:
		resp.Get = &kvrpcpb.GetResponse{
			RegionError: e,
		}
	case CmdScan:
		resp.Scan = &kvrpcpb.ScanResponse{
			RegionError: e,
		}
	case CmdPrewrite:
		resp.Prewrite = &kvrpcpb.PrewriteResponse{
			RegionError: e,
		}
	case CmdCommit:
		resp.Commit = &kvrpcpb.CommitResponse{
			RegionError: e,
		}
	case CmdCleanup:
		resp.Cleanup = &kvrpcpb.CleanupResponse{
			RegionError: e,
		}
	case CmdBatchGet:
		resp.BatchGet = &kvrpcpb.BatchGetResponse{
			RegionError: e,
		}
	case CmdBatchRollback:
		resp.BatchRollback = &kvrpcpb.BatchRollbackResponse{
			RegionError: e,
		}
	case CmdScanLock:
		resp.ScanLock = &kvrpcpb.ScanLockResponse{
			RegionError: e,
		}
	case CmdResolveLock:
		resp.ResolveLock = &kvrpcpb.ResolveLockResponse{
			RegionError: e,
		}
	case CmdGC:
		resp.GC = &kvrpcpb.GCResponse{
			RegionError: e,
		}
	case CmdDeleteRange:
		resp.DeleteRange = &kvrpcpb.DeleteRangeResponse{
			RegionError: e,
		}
	case CmdRawGet:
		resp.RawGet = &kvrpcpb.RawGetResponse{
			RegionError: e,
		}
	case CmdRawPut:
		resp.RawPut = &kvrpcpb.RawPutResponse{
			RegionError: e,
		}
	case CmdRawDelete:
		resp.RawDelete = &kvrpcpb.RawDeleteResponse{
			RegionError: e,
		}
	case CmdRawScan:
		resp.RawScan = &kvrpcpb.RawScanResponse{
			RegionError: e,
		}
	case CmdCop:
		resp.Cop = &CopResponse{First: coprocessor.Response{
			RegionError: e,
		},
		}
	case CmdMvccGetByKey:
		resp.MvccGetByKey = &kvrpcpb.MvccGetByKeyResponse{
			RegionError: e,
		}
	case CmdMvccGetByStartTs:
		resp.MvccGetByStartTS = &kvrpcpb.MvccGetByStartTsResponse{
			RegionError: e,
		}
	case CmdSplitRegion:
		resp.SplitRegion = &kvrpcpb.SplitRegionResponse{
			RegionError: e,
		}
	default:
		return nil, fmt.Errorf("invalid request type %v", req.Type)
	}
	return resp, nil
}

// GetRegionError returns the RegionError of the underlying concrete response.
func (resp *Response) GetRegionError() (*errorpb.Error, error) {
	var e *errorpb.Error
	switch resp.Type {
	case CmdGet:
		e = resp.Get.GetRegionError()
	case CmdScan:
		e = resp.Scan.GetRegionError()
	case CmdPrewrite:
		e = resp.Prewrite.GetRegionError()
	case CmdCommit:
		e = resp.Commit.GetRegionError()
	case CmdCleanup:
		e = resp.Cleanup.GetRegionError()
	case CmdBatchGet:
		e = resp.BatchGet.GetRegionError()
	case CmdBatchRollback:
		e = resp.BatchRollback.GetRegionError()
	case CmdScanLock:
		e = resp.ScanLock.GetRegionError()
	case CmdResolveLock:
		e = resp.ResolveLock.GetRegionError()
	case CmdGC:
		e = resp.GC.GetRegionError()
	case CmdDeleteRange:
		e = resp.DeleteRange.GetRegionError()
	case CmdRawGet:
		e = resp.RawGet.GetRegionError()
	case CmdRawPut:
		e = resp.RawPut.GetRegionError()
	case CmdRawDelete:
		e = resp.RawDelete.GetRegionError()
	case CmdRawScan:
		e = resp.RawScan.GetRegionError()
	case CmdCop:
		e = resp.Cop.First.GetRegionError()
	case CmdMvccGetByKey:
		e = resp.MvccGetByKey.GetRegionError()
	case CmdMvccGetByStartTs:
		e = resp.MvccGetByStartTS.GetRegionError()
	case CmdSplitRegion:
		e = resp.SplitRegion.GetRegionError()
	default:
		return nil, fmt.Errorf("invalid response type %v", resp.Type)
	}
	return e, nil
}
