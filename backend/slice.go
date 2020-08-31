// Copyright 2016 The kingshard Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"): you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

// Copyright 2019 The Gaea Authors. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package backend

import (
	"context"
	"strconv"
	"strings"
	"sync"

	"github.com/ZzzYtl/MyMask/log"
	"github.com/ZzzYtl/MyMask/models"
	"github.com/ZzzYtl/MyMask/mysql"
	"github.com/ZzzYtl/MyMask/util"
)

const (
	weightSplit  = "@"
	DefaultSlice = "slice-0"
)

// Slice means one slice of the mysql cluster
type Slice struct {
	Cfg models.Slice

	sync.RWMutex
	Slave          []*ConnectionPool
	LastSlaveIndex int
	RoundRobinQ    []int
	SlaveWeights   []int

	LastStatisticSlaveIndex   int
	StatisticSlaveRoundRobinQ []int
	StatisticSlaveWeights     []int

	charset     string
	collationID mysql.CollationID
}

// GetConn get backend connection from different node based on fromSlave and userType
func (s *Slice) GetConn(userType int) (pc *PooledConnection, err error) {
	pc, err = s.GetSlaveConn()
	if err != nil {
		log.Warn("get connection from backend failed, error: %s", err.Error())
		return
	}
	return
}

// GetSlaveConn return a connection in slave pool
func (s *Slice) GetSlaveConn() (*PooledConnection, error) {
	s.Lock()
	cp, err := s.getNextSlave()
	s.Unlock()
	if err != nil {
		return nil, err
	}
	ctx := context.TODO()
	return cp.Get(ctx)
}

// Close close the pool in slice
func (s *Slice) Close() error {
	s.Lock()
	defer s.Unlock()

	// close slaves
	for i := range s.Slave {
		s.Slave[i].Close()
	}
	return nil
}

// ParseSlave create connection pool of slaves
// (127.0.0.1:3306@2,192.168.0.12:3306@3)
func (s *Slice) ParseSlave(slaves []string) error {
	if len(slaves) == 0 {
		return nil
	}

	var err error
	var weight int

	count := len(slaves)
	s.Slave = make([]*ConnectionPool, 0, count)
	s.SlaveWeights = make([]int, 0, count)

	//parse addr and weight
	for i := 0; i < count; i++ {
		addrAndWeight := strings.Split(slaves[i], weightSplit)
		if len(addrAndWeight) == 2 {
			weight, err = strconv.Atoi(addrAndWeight[1])
			if err != nil {
				return err
			}
		} else {
			weight = 1
		}
		s.SlaveWeights = append(s.SlaveWeights, weight)
		idleTimeout, err := util.Int2TimeDuration(s.Cfg.IdleTimeout)
		if err != nil {
			return err
		}
		cp := NewConnectionPool(addrAndWeight[0], s.Cfg.UserName, s.Cfg.Password, "", s.Cfg.Capacity, s.Cfg.MaxCapacity, idleTimeout, s.charset, s.collationID)
		cp.Open()
		s.Slave = append(s.Slave, cp)
	}
	s.initBalancer()
	return nil
}

// SetCharsetInfo set charset
func (s *Slice) SetCharsetInfo(charset string, collationID mysql.CollationID) {
	s.charset = charset
	s.collationID = collationID
}
