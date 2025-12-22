## CSE 535: Distributed Systems

Implemented a fault-tolerant distributed transaction system with sharded key-value storage, replicating data.
End-to-end transaction execution (read-only, intra-shard, cross-shard) with isolation, WAL recovery, failure handling, and history-based offline resharding to minimize cross-shard overhead and improve cluster balance.
