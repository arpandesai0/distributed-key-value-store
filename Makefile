run: build
	@./bin/distributed-key-value-store --db-location=db/my1.db --shard=A
build:
	@go build -o bin/distributed-key-value-store