package twopc

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strconv"
	"time"
)

func (s *Server) ClientReadRequest(ctx context.Context, clientReadReq *ClientReadReq) (*ClientReadResp, error) {
	if !s.IsAvailable {
		return nil, s.IsNotAvailable()
	}
	if !s.IsLeader {
		if s.CurrLeaderBallot.ProcessID != 0 && s.CurrLeaderBallot.ProcessID != int32(s.Id) {
			log.Printf("node %v is not the leader. Redirecting read request to node %v", s.Id, s.CurrLeaderBallot.ProcessID)
			ctx, _ := context.WithTimeout(context.Background(), 3*time.Second)
			return s.GrpcClientMap[Nodes[int(s.CurrLeaderBallot.ProcessID)]].ClientReadRequest(ctx, clientReadReq)
		} else {
			return nil, errors.New("node is not aware of the leader")
		}
	} else {
		bal, err := s.Datastore.GetValue(clientReadReq.Client, s.Datastore.Server)
		if err != nil {
			fmt.Printf("Could not get balance for client %v: %v", clientReadReq.Client, err)
			return nil, err
		}
		balint, _ := strconv.Atoi(string(bal))
		return &ClientReadResp{Balance: int32(balint), Ballot: s.CurrLeaderBallot}, nil
	}
}
