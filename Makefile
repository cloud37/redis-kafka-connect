
VERSION := $(shell cat VERSION)

# Default task, will run both build and copy
all: clean build copy

# Build task
build:
	./gradlew createConfluentArchive

# Copy task
copy: build
	cp core/redis-kafka-connect/build/confluent/redis-redis-kafka-connect-$(VERSION).zip ../bdl-datapool-filter/kafka/kafka-connect/
	cp core/redis-kafka-connect/build/confluent/redis-redis-kafka-connect-$(VERSION).zip ../opus.bdl.datapool.infra/kafka/kafka-connect/

# Clean task (optional), if you want to clean build artifacts
clean:
	./gradlew clean

# Phony targets to avoid conflicts with any actual files named 'all', 'build', 'copy', or 'clean'
.PHONY: all build copy clean
