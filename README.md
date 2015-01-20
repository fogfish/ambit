# ambit

The library provides highly available and eventual consistent process management layer using Erlang distribution. It uses combination of well known techniques to achieve scalability and availability such as consistent hashing, *object version* and anti-entropy. 

In contrast with traditional key/value stores, the library provides simple read (whereis) and write (spawn) operations to Erlang process that is uniquely identified by key. Each process is finite-state machine that independently progress its own state once it is spawned. 



## system architecture

The library employs very complex architecture solution. It require variety of software management layers in additional to process (actor) management. The ambit library focuses on partitioning, replication, versioning, failure handling and scaling

* _concurrency_ and _job scheduling_ is implemented using Erlang run-time

* _request marshalling_ and _request routing_ is implemented using Erlang distribution, which is abstracted using pipe library

* _membership_ and _failure detection_ is provided by erlang kluster library.

* _failure recovery_ is based on Erlang supervisor framework.

* _load balancing_ is based on peer-to-peer architectural design

* _replica synchronization_ and _state transfer_ is implemented using anti-entropy protocols

* _overload handling_

* _system monitoring_ and _alarming_

* _configuration management_ 



## system interface

The library associates processes with a key and provides life cycle management through simple interface:

* _spawn_ - the operation locates nodes associated with the key in the Erlang cluster and spawns the process(es) on them (using supplied specification). The process specification is not mutable during process life cycle.

* _free_ - the operation locates nodes associated with key and terminates these process(es).

* _whereis_ - the operation discovers process identifiers in the Erlang cluster using supplied key.



## cluster partition

The library uses consistent hashing algorithm to bind process with cluster replicas. It ensures a fixed mapping of key to corresponding replica (for given set of cluster nodes); it also guaranties that only ```k / s``` processes needs to be relocated on average when new node joins a cluster.    

Let's have a finite field of integers on modulo 2^m, they form a ring. The ring defines a function that place any integer to ring:

A(x) -> x mod 2^m

The ring is divided on q equally sized ranges (so called virtual nodes). Each v-node holds equal amount of keys (cardinality) and it has an address - the first integer that belongs to the range:

V(x) -> x * 2^m div q

There is a v-node allocator function that assigns it to physical node when node joins or leaves cluster. Each node hold on average ```q / s``` v-nodes. 

The ring defines a successors and predecessors membership functions for any term. It maps term to v-node using SHA-1 to calculate the digest as 2^160 integer and place it to ring using map function A. Predecessors is v-nodes with address smaller then address of term and successors - with greater address.

The consistent hashing is also used to replicate data on multiple hosts. Each key is assigned to v-node and it is describe above. The library replicates entire v-node content to n distinct successor Erlang nodes in the ring (it iterates and skips successor v-nodes and accumulates physical node mapping until n-distinct nodes requirements is fulfilled). Every node in system can determine successor list for any key. 



## process version

tbd


## coordinator

The library nominates a coordinator node for each read and write operation executed in cluster. The coordinator is the first primary node in the successor list. The request is always delegated to coordinator event it request is issued to any random cluster node. 

The coordinator executes operation locally and replicates it to n - 1 successor nodes in the ring.

(sloppy quorum, R + W > N)



## hinted hand-off

tbd



# todo

1. object version, vector clock to manage process state. 

1. sloppy quorum.



