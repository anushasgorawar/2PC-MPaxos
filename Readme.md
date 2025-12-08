## CSE 535: Distributed Systems
## Project 3
## Due: December 07, 2025 (11:59 pm)

go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
export PATH="$PATH:$(go env GOPATH)/bin"

protoc --go_out=. --go-grpc_out=. twopc.proto