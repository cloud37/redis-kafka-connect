# Default task, will run both build and copy
all: build copy

# Build task
build:
	./gradlew createConfluentArchive

# Copy task
copy: build
	cp core/redis-kafka-connect/build/confluent/redis-redis-kafka-connect-0.11.0-beta.zip ../bdl-datapool-filter/kafka/kafka-connect/
	cp core/redis-kafka-connect/build/confluent/redis-redis-kafka-connect-0.11.0-beta.zip ../opus.bdl.datapool.infra/kafka/kafka-connect/

# Clean task (optional), if you want to clean build artifacts
clean:
	./gradlew clean

# Phony targets to avoid conflicts with any actual files named 'all', 'build', 'copy', or 'clean'
.PHONY: all build copy clean
