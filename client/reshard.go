package main

import (
	"fmt"
	"log"

	twopc "github.com/F25-CSE535/2pc-anushasgorawar/twopc"
	"github.com/yourbasic/graph"
)

func GetCurrTransactions(sets [][]*twopc.Transaction) []*twopc.Transaction {
	transactions := []*twopc.Transaction{}
	for _, i := range sets {
		for _, j := range i {
			if j.Amount != 0 {
				transactions = append(transactions, j)
			}
		}
	}
	return transactions
}
func ComputeMoves(oldMap, newMap map[string]int) []string {
	moves := []string{}
	for id, oldC := range oldMap {
		newC := newMap[id]
		if oldC != newC {
			moves = append(moves, fmt.Sprintf("(%s, c%d, c%d)", id, oldC, newC))
		}
	}
	return moves
}
func PrintReshard(sets [][]*twopc.Transaction) {
	log.Println("Resharding")
	currTransactions := GetCurrTransactions(sets)
	log.Println("Previous Set Transactions: ")
	log.Println(currTransactions)

	newshards := Reshard(currTransactions, ShardMap, 3)
	moves := ComputeMoves(ShardMap, newshards)
	fmt.Println(moves)
}

// Transaction structure assumed:
type Transaction struct {
	Sender   string
	Reciever string
	Amount   int
}
func Reshard(transactions []*twopc.Transaction, oldMap map[string]int, numClusters int) map[string]int {

	// 1. String ID → index mapping
	idToIndex := make(map[string]int)
	indexToId := make([]string, 0)

	makeIndex := func(id string) int {
		if idx, ok := idToIndex[id]; ok {
			return idx
		}
		idx := len(indexToId)
		idToIndex[id] = idx
		indexToId = append(indexToId, id)
		return idx
	}

	// Build mapping only for IDs in transactions
	for _, t := range transactions {
		makeIndex(t.Sender)
		makeIndex(t.Reciever)
	}

	// ❗ FIX: graph only uses actual number of used IDs
	g := graph.New(len(indexToId))

	// 2. Add edges only among IDs that appear in transactions
	for _, t := range transactions {
		s := idToIndex[t.Sender]
		r := idToIndex[t.Reciever]
		g.AddBoth(s, r)
	}

	// 3. Connected components
	components := graph.Components(g)

	newMap := make(map[string]int)
	clusterID := 1

	for _, comp := range components {
		if len(comp) == 0 {
			continue
		}

		// Assign entire connected component to same cluster
		for _, idx := range comp {
			id := indexToId[idx]  
			newMap[id] = clusterID
		}

		clusterID++
		if clusterID > numClusters {
			clusterID = 1
		}
	}

	// 4. IDs never appearing in any transaction → keep old placement
	for id, oldC := range oldMap {
		if _, seen := newMap[id]; !seen {
			newMap[id] = oldC
		}
	}

	return newMap
}
