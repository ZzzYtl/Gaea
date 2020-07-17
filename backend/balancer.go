package backend

import (
	"math/rand"
	"time"

	"github.com/ZzzYtl/MyMask/core/errors"
)

// calculate gcd ?
func gcd(ary []int) int {
	var i int
	min := ary[0]
	length := len(ary)
	for i = 0; i < length; i++ {
		if ary[i] < min {
			min = ary[i]
		}
	}

	for {
		isCommon := true
		for i = 0; i < length; i++ {
			if ary[i]%min != 0 {
				isCommon = false
				break
			}
		}
		if isCommon {
			break
		}
		min--
		if min < 1 {
			break
		}
	}
	return min
}

// initBalancer init balancer of slaves
func (s *Slice) initBalancer() {
	var sum int
	s.LastSlaveIndex = 0
	gcd := gcd(s.SlaveWeights)

	for _, weight := range s.SlaveWeights {
		sum += weight / gcd
	}

	s.RoundRobinQ = make([]int, 0, sum)
	for index, weight := range s.SlaveWeights {
		for j := 0; j < weight/gcd; j++ {
			s.RoundRobinQ = append(s.RoundRobinQ, index)
		}
	}

	//random order
	if 1 < len(s.SlaveWeights) {
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		for i := 0; i < sum; i++ {
			x := r.Intn(sum)
			temp := s.RoundRobinQ[x]
			other := sum % (x + 1)
			s.RoundRobinQ[x] = s.RoundRobinQ[other]
			s.RoundRobinQ[other] = temp
		}
	}
}

// initStatisticSlaveBalancer init balancer of statistic slaves
func (s *Slice) initStatisticSlaveBalancer() {
	var sum int
	s.LastStatisticSlaveIndex = 0
	gcd := gcd(s.StatisticSlaveWeights)

	for _, weight := range s.StatisticSlaveWeights {
		sum += weight / gcd
	}

	s.StatisticSlaveRoundRobinQ = make([]int, 0, sum)
	for index, weight := range s.StatisticSlaveWeights {
		for j := 0; j < weight/gcd; j++ {
			s.StatisticSlaveRoundRobinQ = append(s.StatisticSlaveRoundRobinQ, index)
		}
	}

	//random order
	if 1 < len(s.StatisticSlaveWeights) {
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		for i := 0; i < sum; i++ {
			x := r.Intn(sum)
			temp := s.StatisticSlaveRoundRobinQ[x]
			other := sum % (x + 1)
			s.StatisticSlaveRoundRobinQ[x] = s.StatisticSlaveRoundRobinQ[other]
			s.StatisticSlaveRoundRobinQ[other] = temp
		}
	}
}

// getNextSlave return connection pool of calculated ip
func (s *Slice) getNextSlave() (*ConnectionPool, error) {
	var index int
	queueLen := len(s.RoundRobinQ)
	if queueLen == 0 {
		return nil, errors.ErrNoDatabase
	}
	if queueLen == 1 {
		index = s.RoundRobinQ[0]
		return s.Slave[index], nil
	}

	s.LastSlaveIndex = s.LastSlaveIndex % queueLen
	index = s.RoundRobinQ[s.LastSlaveIndex]
	if len(s.Slave) <= index {
		return nil, errors.ErrNoDatabase
	}
	cp := s.Slave[index]
	s.LastSlaveIndex++
	s.LastSlaveIndex = s.LastSlaveIndex % queueLen
	return cp, nil
}
