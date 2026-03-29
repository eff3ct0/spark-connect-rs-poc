# Spark Connect Rust - PoC

Proof of concept for the Apache Spark Connect Rust client using features from [rafafrdz/spark-connect-rust](https://github.com/rafafrdz/spark-connect-rust) `dev` branch.

## Features tested

1. Session creation with configurable gRPC timeouts
2. Basic SQL execution
3. Parameterized SQL (named and positional args)
4. DataFrame creation from Arrow RecordBatch
5. DataFrame transformations (filter, select, sort, alias)
6. Aggregations (count, avg, sum, min, max)
7. Parquet write/read
8. Batch-by-batch result iteration (`to_local_iterator`)
9. Higher-order functions (transform, aggregate with lambdas)
10. Checkpointing
11. Observe (metric collection)
12. Catalog operations
13. Runtime configuration
14. Session stop

## Prerequisites

- Rust 1.81+
- Docker
- `protoc` (protobuf compiler)

## Run

```bash
# Start Spark Connect server
docker compose up -d

# Wait ~30s for Spark to start, then run the PoC
cargo run

# Stop Spark
docker compose down
```
