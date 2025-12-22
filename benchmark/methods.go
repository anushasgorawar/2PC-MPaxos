package main

import (
	"math/rand"
	"strconv"
	"time"

	"github.com/anushasgorawar/2PC-MPaxos/twopc"
)

type BenchmarkConfig struct {
	TotalOperations int
	ReadWriteRatio  float64
	CrossShardRatio float64
	Skew            float64
}

type BenchmarkResult struct {
	Reads                int
	TransferTransactions int
	AvgTransferLatency   time.Duration
	Throughput           float64
}

func randUniform(min, max int) int {
	return rand.Intn(max-min+1) + min
}

func randSkewed(min, max int) int {
	// 90% probability pick from hot range
	hotSize := (max - min + 1) / 100 // top 1%
	hotMin := min
	hotMax := min + hotSize

	if rand.Float64() < 0.9 {
		return randUniform(hotMin, hotMax)
	}
	return randUniform(min, max)
}

func pickIDWithSkew(skew float64) int {
	if skew < 0 {
		skew = 0
	}
	if skew > 1 {
		skew = 1
	}

	min, max := 1, 9000
	hotFraction := 0.01 // top 1% as hotspot
	hotSize := int(float64(max-min+1) * hotFraction)
	if hotSize < 1 {
		hotSize = 1
	}
	hotMin := min
	hotMax := min + hotSize - 1

	if rand.Float64() < skew {
		// pick from hotspot
		return randUniform(hotMin, hotMax)
	}
	// pick uniformly from full space
	return randUniform(min, max)
}

func CreateWorkload(total int, readWriteRatio, crossShardRatio, skew float64) []*twopc.Transaction {
	workload := make([]*twopc.Transaction, 0, total)

	numReadWrite := int(float64(total) * readWriteRatio)
	numReadOnly := total - numReadWrite
	numCrossShard := int(float64(numReadWrite) * crossShardRatio)
	numIntraShard := numReadWrite - numCrossShard
	for i := 0; i < numReadOnly; i++ {
		s := pickIDWithSkew(skew)
		workload = append(workload, &twopc.Transaction{
			Sender: strconv.Itoa(s),
		})
	}
	for i := 0; i < numIntraShard; i++ {
		s := pickIDWithSkew(skew)
		shardS := GetClusterID(strconv.Itoa(s))
		var r int
		for {
			r = randUniform(1, 9000)
			if GetClusterID(strconv.Itoa(r)) == shardS && r != s {
				break
			}
		}

		amount := int32(randUniform(1, 10))
		workload = append(workload, &twopc.Transaction{
			Sender:   strconv.Itoa(s),
			Reciever: strconv.Itoa(r),
			Amount:   amount,
		})
	}
	for i := 0; i < numCrossShard; i++ {
		s := pickIDWithSkew(skew)
		shardS := GetClusterID(strconv.Itoa(s))
		var r int
		for {
			r = randUniform(1, 9000)
			if GetClusterID(strconv.Itoa(r)) != shardS {
				break
			}
		}

		amount := int32(randUniform(1, 10))
		workload = append(workload, &twopc.Transaction{
			Sender:   strconv.Itoa(s),
			Reciever: strconv.Itoa(r),
			Amount:   amount,
		})
	}
	rand.Shuffle(len(workload), func(i, j int) {
		workload[i], workload[j] = workload[j], workload[i]
	})

	return workload
}
