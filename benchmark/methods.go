package main

import (
	"math/rand"
	"strconv"
	"time"

	"github.com/F25-CSE535/2pc-anushasgorawar/twopc"
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

func getShard(id int) int {
	switch {
	case id >= 1 && id <= 3000:
		return 1
	case id >= 3001 && id <= 6000:
		return 2
	case id >= 6001 && id <= 9000:
		return 3
	default:
		return -1
	}
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

	// 1. Read-only: (s) → Reciever empty, Amount 0
	for i := 0; i < numReadOnly; i++ {
		id := pickIDWithSkew(skew)
		workload = append(workload, &twopc.Transaction{
			Sender: strconv.Itoa(id),
			// Reciever: "",
			// Amount:   0,
		})
	}

	// 2. Intra-shard read-write
	for i := 0; i < numIntraShard; i++ {
		s := pickIDWithSkew(skew)
		shard := getShard(s)

		var r int
		for {
			r = pickIDWithSkew(skew)
			if getShard(r) == shard {
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

	// 3. Cross-shard read-write
	for i := 0; i < numCrossShard; i++ {
		s := pickIDWithSkew(skew)
		var r int
		for {
			r = pickIDWithSkew(skew)
			if getShard(r) != getShard(s) {
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

	// Optional: shuffle to mix reads, intra, cross
	rand.Shuffle(len(workload), func(i, j int) {
		workload[i], workload[j] = workload[j], workload[i]
	})

	return workload
}
