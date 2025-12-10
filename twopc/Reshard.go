package twopc

import (
	"context"
	"log"
	"time"
)

func (s *Server) Reshard(ctx context.Context, records *Records) (*Empty, error) {
	//Create database to the server
	start := time.Now
	s.Clients = s.CreateClients()
	var err error
	log.Printf("Resharding..")
	AddtoThisCluster := []string{}
	RemoveFromThisCluster := []string{}
	for _, record := range records.Records {
		if record.Newcluster == int32(s.Id) {
			AddtoThisCluster = append(AddtoThisCluster, record.Client)
		}
		if record.OldCluster == int32(s.Id) {
			RemoveFromThisCluster = append(RemoveFromThisCluster, record.Client)
		}
	}
	err = s.Datastore.Reshard(AddtoThisCluster, RemoveFromThisCluster)
	if err != nil {
		return &Empty{}, err
	}
	log.Println("Done Resharding. Time taken:", time.Since(start()))

	return &Empty{}, nil
}
