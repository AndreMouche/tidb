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

package util

import (
	"flag"
	"sync"

	"github.com/pingcap/check"
)

var (
	withTiKVGlobalLock sync.RWMutex
	withTiKV           = flag.Bool("with-tikv", false, "run tests with TiKV cluster started. (not use the mock server)")
)

// OneByOneSuite is a suite, When with-tikv flag is true, there is only one storage, so the test suite have to run one by one.
type OneByOneSuite struct{}

// SetUpSuite implement the Suite interface.
func (s *OneByOneSuite) SetUpSuite(c *check.C) {
	if *withTiKV {
		withTiKVGlobalLock.Lock()
	} else {
		withTiKVGlobalLock.RLock()
	}
}

// TearDownSuite implement the Suite interface.
func (s *OneByOneSuite) TearDownSuite(c *check.C) {
	if *withTiKV {
		withTiKVGlobalLock.Unlock()
	} else {
		withTiKVGlobalLock.RUnlock()
	}
}
