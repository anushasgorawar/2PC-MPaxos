## CSE 535: Distributed Systems - 2PC-MPaxos

A fault-tolerant distributed transaction system with sharded key-value storage, replicating data.

Developed a variant of the Paxos consensus protocol - **Stable Leader Paxos** using gRPC and protocol buffers for inter-node and client communication, reducing leader election overhead and improving throughput, and **2PC** for inter-cluster transactions.

Built end-to-end support for read-only, intra-shard, and cross-shard transactions, including Isolation, write-ahead logging (WAL), abort recovery, and timeout handling under node failures and partial cluster outages across multiple clusters.

Developed an offline resharding mechanism using transaction-history-based partitioning, relocating thousands of hot data items across clusters to reduce cross-shard transactions and balance load.


