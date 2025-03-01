#!/bin/bash
set -e  # Exit if any command fails

# Trap SIGINT (Ctrl+C) to stop all instances on exit
trap 'pkill -f bin/distributed-key-value-store' SIGINT

cd "$(dirname "$0")"  # Change to the script's directory

# Ensure the bin directory exists
mkdir -p bin

# Build the binary
echo "Building the binary..."
go build -o bin/distributed-key-value-store

# Kill any existing instances
pkill -f bin/distributed-key-value-store || true
sleep 0.1

# Start the distributed key-value store instances
echo "Starting distributed key-value store instances..."
bin/distributed-key-value-store --db-location=a.db --http-addr=127.0.0.1:8080 --config-file=sharding.toml -shard=A &
bin/distributed-key-value-store --db-location=a.db --http-addr=127.0.0.11:8080 --config-file=sharding.toml -shard=A -replica &

bin/distributed-key-value-store --db-location=b.db --http-addr=127.0.0.2:8080 --config-file=sharding.toml -shard=B &
bin/distributed-key-value-store --db-location=a.db --http-addr=127.0.0.22:8080 --config-file=sharding.toml -shard=B -replica &

bin/distributed-key-value-store --db-location=c.db --http-addr=127.0.0.3:8080 --config-file=sharding.toml -shard=C &
bin/distributed-key-value-store --db-location=a.db --http-addr=127.0.0.33:8080 --config-file=sharding.toml -shard=C -replica &

bin/distributed-key-value-store --db-location=c.db --http-addr=127.0.0.4:8080 --config-file=sharding.toml -shard=C &
bin/distributed-key-value-store --db-location=a.db --http-addr=127.0.0.44:8080 --config-file=sharding.toml -shard=C -replica &


wait  # Wait for all background processes to finish
