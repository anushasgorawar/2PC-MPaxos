package main

import (
	"os"
	"regexp"
	"strconv"
	"strings"

	twopc "github.com/F25-CSE535/2pc-anushasgorawar/twopc"
)

func ReadTransactions(filePath string) ([][][]*twopc.Transaction, [][]int, error) {

	rawBytes, err := os.ReadFile(filePath)
	if err != nil {
		return nil, nil, err
	}

	lines := strings.Split(string(rawBytes), "\n")

	reSetNum := regexp.MustCompile(`^(\d+)\s*,`)
	reNodes := regexp.MustCompile(`\[(.*?)\]`)
	reTriple := regexp.MustCompile(`\((\d+)\s*,\s*(\d+)\s*,\s*(\d+)\)`)
	reSingle := regexp.MustCompile(`\((\d+)\)`)
	reOp := regexp.MustCompile(`([FR])\(n(\d+)\)`)
	reToken := regexp.MustCompile(`([FR]\(n\d+\)|\([^)]*\))`)

	var allSets [][][]*twopc.Transaction
	var liveNodes [][]int

	var segments [][]*twopc.Transaction
	var currentSegment []*twopc.Transaction
	inSet := false

	flushSet := func() {
		if len(currentSegment) > 0 {
			segments = append(segments, currentSegment)
		}
		if len(segments) > 0 {
			allSets = append(allSets, segments)
		}
		segments = [][]*twopc.Transaction{}
		currentSegment = []*twopc.Transaction{}
	}

	for _, raw := range lines {
		line := strings.TrimSpace(raw)
		if line == "" {
			continue
		}

		// NEW SET NUMBER
		if m := reSetNum.FindStringSubmatch(line); m != nil {
			if inSet {
				flushSet()
			}
			inSet = true
		}

		// LIVE NODES
		if m := reNodes.FindStringSubmatch(line); m != nil {
			rawlist := m[1]
			parts := strings.Split(rawlist, ",")
			var ids []int
			for _, p := range parts {
				p = strings.TrimSpace(p)
				p = strings.TrimPrefix(p, "n")
				if v, err := strconv.Atoi(p); err == nil {
					ids = append(ids, v)
				}
			}
			liveNodes = append(liveNodes, ids)
		}

		// PARSE TOKENS
		tokens := reToken.FindAllString(line, -1)

		for _, tok := range tokens {

			// F(nX) or R(nX)
			if m := reOp.FindStringSubmatch(tok); m != nil {
				// close current segment
				if len(currentSegment) > 0 {
					segments = append(segments, currentSegment)
				}
				currentSegment = []*twopc.Transaction{}
				segments = append(segments, []*twopc.Transaction{
					{
						Sender:   m[1], // "F" or "R"
						Reciever: m[2],
						Amount:   0,
					},
				})
				continue
			}

			// (a, b, c)
			if m := reTriple.FindStringSubmatch(tok); m != nil {
				amt, _ := strconv.Atoi(m[3])
				currentSegment = append(currentSegment, &twopc.Transaction{
					Sender:   m[1],
					Reciever: m[2],
					Amount:   int32(amt),
				})
				continue
			}

			// (X)
			if m := reSingle.FindStringSubmatch(tok); m != nil {
				currentSegment = append(currentSegment, &twopc.Transaction{
					Sender:   m[1],
					Reciever: "",
					Amount:   0,
				})
				continue
			}
		}
	}

	if inSet {
		flushSet()
	}

	return allSets, liveNodes, nil
}
