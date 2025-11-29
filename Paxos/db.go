package paxos

import (
	"context"
	"log"
)

func (s *Server) PrintDB(ctx context.Context, empty *Empty) (*AllBalance, error) {
	allBalances, err := s.Datastore.PrintDB()
	if err != nil {
		log.Println("Could not PrintDB")
		return nil, err
	}
	all := &AllBalance{}
	for _, balance := range allBalances {
		all.Balance = append(all.Balance, &Balance{Balance: balance})
	}
	return all, nil
}

func (s *Server) PrintBalance(ctx context.Context, clientID *ClientID) (*Balance, error) {
	balance, err := s.Datastore.GetValue(clientID.ClientID, s.Datastore.Server)
	if err != nil {
		log.Println("Error fetching balance of client: ", clientID.ClientID)
	}
	return &Balance{Balance: string(balance)}, nil
}
