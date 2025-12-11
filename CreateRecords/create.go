package main

import (
	"encoding/json"
	"os"
)

func main() {
	filename := "records.json"
	shardMap := make(map[int]int, 9000)

	for i := 1; i <= 3000; i++ {
		shardMap[i] = 1
	}

	for i := 3001; i <= 6000; i++ {
		shardMap[i] = 2
	}

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
