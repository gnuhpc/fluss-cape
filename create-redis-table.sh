#!/bin/bash
#
# Script to create Redis data table in Fluss
#

FLUSS_BOOTSTRAP=${1:-"localhost:9123"}

echo "Creating Redis data table in Fluss..."
echo "Bootstrap servers: $FLUSS_BOOTSTRAP"

# Using Java command to create table via Fluss Admin API
java -cp "target/fluss-hbase-compat-1.0.0-SNAPSHOT.jar" \
  org.apache.fluss.redis.tools.CreateRedisTable \
  "$FLUSS_BOOTSTRAP"

if [ $? -eq 0 ]; then
    echo "✓ Table created successfully!"
    exit 0
else
    echo "✗ Failed to create table"
    exit 1
fi
