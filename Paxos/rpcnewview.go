package paxos

import (
	"context"
)

// FIXME: method1. leader requests for accept logs. client sends accept logs.
func (s *Server) NewViewRequest(ctx context.Context, newViewReq *NewViewReq) (*NewView, error) {
	if !s.IsAvailable {
		return nil, s.IsNotAvailable()
	}
	acceptLogs, err := s.Datastore.GetAllLogs()
	if err != nil {
		return nil, err
	}
	return &NewView{
		AcceptLog: acceptLogs,
	}, nil
}
