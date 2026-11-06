This document lists users of RocksDB and their use cases. If you are using RocksDB, please open a pull request and add yourself to the list.

## Facebook
At Facebook, we use RocksDB as storage engines in multiple data management services and a backend for many different stateful services, including:

1. MyRocks -- https://github.com/MySQLOnRocksDB/mysql-5.6
2. MongoRocks -- https://github.com/mongodb-partners/mongo-rocks
3. ZippyDB --  Facebook's distributed key-value store with Paxos-style replication, built on top of RocksDB.[1] https://www.youtube.com/watch?v=DfiN7pG0D0khtt
4. Laser -- Laser is a high query throughput, low (millisecond) latency, key-value storage service built on top of RocksDB.[1]
4. Dragon -- a distributed graph query engine. https://code.facebook.com/posts/1737605303120405/dragon-a-distributed-graph-query-engine/
5. Stylus -- a low-level stream processing framework written in C++.[1]
6. LogDevice -- a distributed data store for logs [2]

[1] https://research.facebook.com/publications/realtime-data-processing-at-facebook/

[2] https://code.facebook.com/posts/357056558062811/logdevice-a-distributed-data-store-for-logs/

## Bilibili
[Bilibili](bilibili.com) [uses](https://www.alluxio.io/blog/when-ai-meets-alluxio-at-bilibili-building-an-efficient-ai-platform-for-data-preprocessing-and-model-training/) Alluxio to speed up its ML training workloads, and Alluxio uses RocksDB to store its filesystem metadata, so Bilibili uses RocksDB.

Bilibili's [real-time platform](https://www.alibabacloud.com/blog/architecture-and-practices-of-bilibilis-real-time-platform_596676) uses Flink, and uses RocksDB as Flink's state store.

## TikTok
TikTok, or its parent company ByteDance, uses RocksDB as the storage engine for some storage systems, such as its distributed graph database [ByteGraph](https://vldb.org/pvldb/vol15/p3306-li.pdf). 

Also, TikTok uses [Alluxio](alluxio.io) to [speed up Presto queries](https://www.alluxio.io/resources/videos/improving-presto-performance-with-alluxio-at-tiktok/), and Alluxio stores the files' metadata in RocksDB.

## FoundationDB
[FoundationDB](https://www.foundationdb.org/) [uses](https://github.com/apple/foundationdb/blob/377f1f692da6ab2fe5bdac57035651db3e5fb66d/fdbserver/KeyValueStoreRocksDB.actor.cpp) RocksDB to implement a [key-value store interface](https://github.com/apple/foundationdb/blob/377f1f692da6ab2fe5bdac57035651db3e5fb66d/fdbserver/KeyValueStoreRocksDB.actor.cpp#L1127) in its server backend.

## Apple
Apple [uses](https://opensource.apple.com/projects/foundationdb/) FoundationDB, so it also uses RocksDB.

## Snowflake
Snowflake [uses](https://www.snowflake.com/blog/how-foundationdb-powers-snowflake-metadata-forward/) FoundationDB, so it also uses RocksDB.

## Microsoft
The Bing search engine from Microsoft uses RocksDB as the storage engine for its web data platform: https://blogs.bing.com/Engineering-Blog/october-2021/RocksDB-in-Microsoft-Bing

## LinkedIn
1. [Venice](https://venicedb.org/) is a derived data platform using RocksDB as its storage engine. It is LinkedIn's ML feature store, powering thousands of recommender use cases, including the Feed, Video recommendations, and People You May Know.
2. LinkedIn's follow feed for storing user's activities. Check out the blog post: https://engineering.linkedin.com/blog/2016/03/followfeed--linkedin-s-feed-made-faster-and-smarter
3. Apache Samza, open source framework for stream processing.

Learn more about LinkedIn's follow feed and Apache Samza in a Tech Talk by Ankit Gupta and Naveen Somasundaram: http://www.youtube.com/watch?v=plqVp_OnSzg

## Yahoo
Yahoo is using RocksDB as a storage engine for their biggest distributed data store Sherpa. Learn more about it here: http://yahooeng.tumblr.com/post/120730204806/sherpa-scales-new-heights

## Tencent
[PaxosStore](https://github.com/Tencent/paxosstore) is a distributed database supporting WeChat. It uses RocksDB as its storage engine.

## Baidu
[Apache Doris](http://doris.apache.org/master/en/) is a MPP analytical database engine released by Baidu. It [uses RocksDB](http://doris.apache.org/master/en/administrator-guide/operation/tablet-meta-tool.html) to manage its tablet's metadata.

## CockroachDB
CockroachDB is an open-source geo-replicated transactional database. They are using RocksDB as their storage engine. Check out their github: https://github.com/cockroachdb/cockroach

## DNANexus
DNANexus is using RocksDB to speed up processing of genomics data.
You can learn more from this great blog post by Mike Lin: http://devblog.dnanexus.com/faster-bam-sorting-with-samtools-and-rocksdb/

## Iron.io
Iron.io is using RocksDB as a storage engine for their distributed queueing system.
Learn more from Tech Talk by Reed Allman: http://www.youtube.com/watch?v=HTjt6oj-RL4

## Tango Me
Tango is using RocksDB as a graph storage to store all users' connection data and other social activity data.

## Turn
Turn is using RocksDB as a storage layer for their key/value store, serving at peak 2.4MM QPS out of different datacenters.
Check out our RocksDB Protobuf merge operator at: https://github.com/vladb38/rocksdb_protobuf

## Santander UK/Cloudera Profession Services
Check out their blog post: http://blog.cloudera.com/blog/2015/08/inside-santanders-near-real-time-data-ingest-architecture/

## Airbnb
Airbnb is using RocksDB as a storage engine for their personalized search service. You can learn more about it here: https://www.youtube.com/watch?v=ASQ6XMtogMs

## Alluxio
[Alluxio](https://www.alluxio.io) uses RocksDB to serve and scale file system metadata to beyond 1 Billion files. The detailed design and implementation is described in this engineering blog:
https://www.alluxio.io/blog/scalable-metadata-service-in-alluxio-storing-billions-of-files/

## Pinterest
Pinterest's Object Retrieval System uses RocksDB for storage: https://www.youtube.com/watch?v=MtFEVEs_2Vo

## Smyte
[Smyte](https://www.smyte.com/) uses RocksDB as the storage layer for their core key-value storage, high-performance counters and time-windowed HyperLogLog services.

## Rakuten Marketing
[Rakuten Marketing](https://marketing.rakuten.com/) uses RocksDB as the disk cache layer for the real-time bidding service in their Performance DSP.

## VWO, Wingify
[VWO's](https://vwo.com/) Smart Code checker and URL helper uses RocksDB to store all the URLs where VWO's Smart Code is installed.

## quasardb
[quasardb](https://www.quasardb.net) is a high-performance, distributed, transactional key-value database that integrates well with in-memory analytics engines such as Apache Spark.
quasardb uses a heavily tuned RocksDB as its persistence layer.

## Netflix
[Netflix](http://techblog.netflix.com/2016/05/application-data-caching-using-ssds.html) Netflix uses RocksDB on AWS EC2 instances with local SSD drives to cache application data.

## TiKV
[TiKV](https://github.com/pingcap/tikv) is a GEO-replicated, high-performance, distributed, transactional key-value database. TiKV is powered by Rust and Raft. TiKV uses RocksDB as its persistence layer.

## TiDB
[TiDB](https://github.com/pingcap/tidb) uses the TiKV distributed key-value database, so it uses RocksDB.

## PingCAP
[PingCAP](https://www.pingcap.com/) is the company behind TiDB, its cloud database service uses RocksDB.

## Apache Spark
[Spark Structured Streaming](https://docs.databricks.com/structured-streaming/rocksdb-state-store.html) uses RocksDB as the local state store.

## Databricks
[Databricks](https://www.databricks.com/) [replaces AWS RDS with TiDB](https://www.pingcap.com/case-study/how-databricks-tackles-the-scalability-limit-with-a-mysql-alternative/) for scalability, so it uses RocksDB.

## Apache Flink
[Apache Flink](https://flink.apache.org/news/2016/03/08/release-1.0.0.html) uses RocksDB to store state locally on a machine.

## Dgraph
[Dgraph](https://github.com/dgraph-io/dgraph) is an open-source, scalable, distributed, low latency, high throughput Graph database .They use RocksDB to store state locally on a machine.

## Uber
[Uber](http://eng.uber.com/cherami/) uses RocksDB as a durable and scalable task queue.

## 360 Pika
[360](http://www.360.cn/) [Pika](https://github.com/Qihoo360/pika) is a nosql compatible with redis. With the huge amount of data stored, redis may suffer for a capacity bottleneck, and pika was born for solving it. It has widely been used in many companies.

## LzLabs
LzLabs is using RocksDB as a storage engine in their multi-database distributed framework to store application configuration and user data.

## ProfaneDB
[ProfaneDB](https://profanedb.gitlab.io/) is a database for Protocol Buffers, and uses RocksDB for storage. It is accessible via gRPC, and the schema is defined using directly `.proto` files.

## IOTA Foundation
 [IOTA Foundation](https://www.iota.org/) is using RocksDB in the [IOTA Reference Implementation (IRI)](https://github.com/iotaledger/iri) to store the local state of the Tangle. The Tangle is the first open-source distributed ledger powering the future of the Internet of Things.

## Avrio Project
 [Avrio Project](http://avrio-project.github.io/avrio.network/) is using RocksDB in [Avrio ](https://github.com/avrio-project/avrio) to store blocks, account balances and data and other blockchain-releated data. Avrio is a multiblockchain decentralized cryptocurrency empowering monetary transactions.

## Crux
[Crux](https://github.com/juxt/crux) is a document database that uses RocksDB for local [EAV](https://en.wikipedia.org/wiki/Entity%E2%80%93attribute%E2%80%93value_model) index storage to enable point-in-time bitemporal Datalog queries. The "unbundled" architecture uses Kafka to provide horizontal scalability.

## Nebula Graph
[Nebula Graph](https://github.com/vesoft-inc/nebula) is a distributed, scalable, lightning-fast, open source graph database capable of hosting super large scale graphs with dozens of billions of vertices (nodes) and trillions of edges, with milliseconds of latency.

## YugabyteDB
[YugabyteDB](https://www.yugabyte.com/) is an open source, high performance, distributed SQL database that uses RocksDB as its storage layer. For more information, please see https://github.com/yugabyte/yugabyte-db/.

## ArangoDB
[ArangoDB](https://www.arangodb.com/) is a native multi-model database with flexible data models for documents, graphs, and key-values, for building high performance applications using a convenient SQL-like query language or JavaScript extensions. It uses RocksDB as its storage engine.

## Qdrant
[Qdrant](https://qdrant.tech/) is an open source vector database, it [uses](https://qdrant.tech/documentation/concepts/storage/) RocksDB as its persistent storage.

## Milvus
[Milvus](https://milvus.io/) is an open source vector database for unstructured data. It uses RocksDB not only as one of the supported kv storage engines, but also as a message queue.

## Kafka
[Kafka](https://kafka.apache.org/) is an open-source distributed event streaming platform, it uses RocksDB to store state in Kafka Streams: https://www.confluent.io/blog/how-to-tune-rocksdb-kafka-streams-state-stores-performance/.

## Solana Labs
[Solana](https://github.com/solana-labs/solana) is a fast, secure, scalable, and decentralized blockchain.  It uses RocksDB as the underlying storage for its ledger store.

## Apache Kvrocks

[Apache Kvrocks](https://github.com/apache/kvrocks) is an open-source distributed key-value NoSQL database built on top of RocksDB. It serves as a cost-saving and capacity-increasing alternative drop-in replacement for Redis.

## Others
More databases using RocksDB can be found at [dbdb.io](https://dbdb.io/browse?embeds=rocksdb).
