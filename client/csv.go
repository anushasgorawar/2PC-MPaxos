package main

import (
	"encoding/csv"
	"os"
	"regexp"
	"strconv"
	"strings"

	paxos "github.com/F25-CSE535/2pc-anushasgorawar/Paxos"
)

var (
	reTriple = regexp.MustCompile(`\((\d+),\s*(\d+),\s*(\d+)\)`)
	reSingle = regexp.MustCompile(`\((\d+)\)`)
	reOp     = regexp.MustCompile(`([A-Z])\(n(\d+)\)`) // R(n6), F(n3)
	reNodes  = regexp.MustCompile(`n(\d+)`)
)

func ReadTransactions(filePath string) ([][][]*paxos.Transaction, [][]int, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return nil, nil, err
	}
	defer f.Close()

	r := csv.NewReader(f)
	r.FieldsPerRecord = -1

	rows, err := r.ReadAll()
	if err != nil {
		return nil, nil, err
	}

	var allSets [][][]*paxos.Transaction // FINAL: sets → groups → tx
	var liveNodes [][]int

	var currentSetGroups [][]*paxos.Transaction
	var currentGroup []*paxos.Transaction
	currentSet := -1

	for _, row := range rows {
		for colIndex, col := range row {
			col = strings.TrimSpace(col)
			if col == "" {
				continue
			}

			// --------------------------
			// Detect new set number
			// --------------------------
			if colIndex == 0 {
				if n, err := strconv.Atoi(col); err == nil {
					// push previous set
					if currentSet != -1 {
						if len(currentGroup) > 0 {
							currentSetGroups = append(currentSetGroups, currentGroup)
						}
						allSets = append(allSets, currentSetGroups)
					}
					// reset
					currentSet = n
					currentSetGroups = [][]*paxos.Transaction{}
					currentGroup = []*paxos.Transaction{}
					continue
				}
			}

			// --------------------------
			// Live nodes
			// --------------------------
			if strings.HasPrefix(col, "[") {
				nodes := reNodes.FindAllStringSubmatch(col, -1)
				var ids []int
				for _, v := range nodes {
					id, _ := strconv.Atoi(v[1])
					ids = append(ids, id)
				}
				liveNodes = append(liveNodes, ids)
				continue
			}

			// --------------------------
			// R(nX) or F(nX) → split here!
			// --------------------------
			if m := reOp.FindStringSubmatch(col); m != nil {
				// close current group
				if len(currentGroup) > 0 {
					currentSetGroups = append(currentSetGroups, currentGroup)
				}
				currentGroup = []*paxos.Transaction{
					{
						Sender:   m[1],
						Reciever: m[2],
						Amount:   0,
					},
				}
				// finalize this as its own group
				currentSetGroups = append(currentSetGroups, currentGroup)
				currentGroup = []*paxos.Transaction{} // start new group after this
				continue
			}

			// --------------------------
			// (a, b, c)
			// --------------------------
			if m := reTriple.FindStringSubmatch(col); m != nil {
				amt, _ := strconv.Atoi(m[3])
				currentGroup = append(currentGroup, &paxos.Transaction{
					Sender:   m[1],
					Reciever: m[2],
					Amount:   int32(amt),
				})
				continue
			}

			// --------------------------
			// (X)
			// --------------------------
			if m := reSingle.FindStringSubmatch(col); m != nil {
				currentGroup = append(currentGroup, &paxos.Transaction{
					Sender:   m[1],
					Reciever: "",
					Amount:   0,
				})
				continue
			}
		}
	}

	// finalize last set
	if len(currentGroup) > 0 {
		currentSetGroups = append(currentSetGroups, currentGroup)
	}
	if len(currentSetGroups) > 0 {
		allSets = append(allSets, currentSetGroups)
	}

	return allSets, liveNodes, nil
}
