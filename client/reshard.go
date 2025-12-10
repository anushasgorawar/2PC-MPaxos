package main

import (
	"sort"
	"strconv"
)

func PrintReshard() {
	// transactions
	//
	//	newshard := Reshard(transactions, ShardMap, 3)
}

// Transaction structure assumed:
type Transaction struct {
	Sender   string
	Reciever string
	Amount   int
}

func Reshard(transactions []*Transaction, oldMap map[int]int, numClusters int) map[int]int {
	// 1. Compute weight of each item based on frequency in transactions
	weights := make(map[int]int)

	for _, t := range transactions {
		if t.Reciever == "" {
			id, _ := strconv.Atoi(t.Sender)
			weights[id]++
			continue
		}

		s, _ := strconv.Atoi(t.Sender)
		r, _ := strconv.Atoi(t.Reciever)
		weights[s]++
		weights[r]++
	}

	// 2. Create a list of items
	type item struct {
		id     int
		weight int
	}

	items := make([]item, 0, 9000)
	for id := 1; id <= 9000; id++ {
		items = append(items, item{id: id, weight: weights[id]})
	}

	// 3. Sort by weight DESC (hot items placed first)
	sort.Slice(items, func(i, j int) bool {
		return items[i].weight > items[j].weight
	})

	// 4. Prepare cluster load counters
	clusterLoad := make([]int, numClusters+1) // 1-indexed: clusterLoad[1], clusterLoad[2], clusterLoad[3]
	newMap := make(map[int]int)

	// 5. Assign each item to the least-loaded cluster
	for _, it := range items {
		bestCluster := 1
		minLoad := clusterLoad[1]

		for c := 2; c <= numClusters; c++ {
			if clusterLoad[c] < minLoad {
				minLoad = clusterLoad[c]
				bestCluster = c
			}
		}

		newMap[it.id] = bestCluster
		clusterLoad[bestCluster]++
	}

	return newMap
}
