package main

import (
	"encoding/json"
	"os"
)

func main() {
	filename := "records.json"
	shardMap := make(map[int]int, 9000)

	// Cluster 1 → IDs 1–3000
	for i := 1; i <= 3000; i++ {
		shardMap[i] = 1
	}

	// Cluster 2 → IDs 3001–6000
	for i := 3001; i <= 6000; i++ {
		shardMap[i] = 2
	}

	// Cluster 3 → IDs 6001–9000
	for i := 6001; i <= 9000; i++ {
		shardMap[i] = 3
	}

	// Marshal with indentation for readability
	data, err := json.MarshalIndent(shardMap, "", "  ")
	if err != nil {
		return
	}

	// Write file
	os.WriteFile(filename, data, 0644)
}
