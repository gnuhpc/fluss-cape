FROM eclipse-temurin:11-jre-jammy

LABEL maintainer="fluss-hbase-compat"
LABEL description="HBase Compatibility Layer for Apache Fluss"

# Install necessary packages
RUN apt-get update && apt-get install -y \
    curl \
    netcat-traditional \
    && rm -rf /var/lib/apt/lists/*

# Create app directory
WORKDIR /app

# Copy the shaded JAR
COPY target/fluss-cape-1.0.0-SNAPSHOT.jar /app/fluss-cape.jar

# Expose ports
# 16020 - HBase RPC port
# 6379 - Redis port
# 5432 - PostgreSQL port
# 9092 - Kafka port
# 8080 - Health check HTTP port
EXPOSE 16020 6379 5432 9092 8080

# Set default environment variables
ENV FLUSS_BOOTSTRAP="localhost:9123"
ENV ZK_QUORUM="localhost:2181"
ENV BIND_ADDRESS="0.0.0.0"
ENV BIND_PORT="16020"
ENV HEALTH_PORT="8080"
ENV TABLES=""
ENV SERVER_ID=""
ENV REDIS_ENABLE="true"
ENV REDIS_BIND_ADDRESS="0.0.0.0"
ENV REDIS_BIND_PORT="6379"
ENV REDIS_SHARDING_ENABLED="true"
ENV REDIS_SHARDING_NUM_SHARDS="16"
ENV PG_ENABLE="true"
ENV PG_BIND_ADDRESS="0.0.0.0"
ENV PG_BIND_PORT="5432"
ENV PG_DATABASE="default"
ENV PG_AUTH_MODE="trust"
ENV KAFKA_ENABLE="true"
ENV KAFKA_BIND_ADDRESS="0.0.0.0"
ENV KAFKA_BIND_PORT="9092"
ENV KAFKA_DEFAULT_DATABASE="default"

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
  CMD curl -f http://localhost:${HEALTH_PORT}/health || exit 1

# Run the application
ENTRYPOINT ["sh", "-c", "java -Xmx2g -Xms1g \
  -Dlog4j.configuration=log4j.properties \
  -Dfluss.bootstrap.servers=${FLUSS_BOOTSTRAP} \
  -Dhbase.zookeeper.quorum=${ZK_QUORUM} \
  -Dhbase.compat.bind.address=${BIND_ADDRESS} \
  -Dhbase.compat.bind.port=${BIND_PORT} \
  -Dhealth.check.port=${HEALTH_PORT} \
  -Dredis.enable=${REDIS_ENABLE} \
  -Dredis.bind.address=${REDIS_BIND_ADDRESS} \
  -Dredis.bind.port=${REDIS_BIND_PORT} \
  -Dredis.sharding.enabled=${REDIS_SHARDING_ENABLED} \
  -Dredis.sharding.num.shards=${REDIS_SHARDING_NUM_SHARDS} \
  -Dpg.enabled=${PG_ENABLE} \
  -Dpg.bind.address=${PG_BIND_ADDRESS} \
  -Dpg.port=${PG_BIND_PORT} \
  -Dpg.database=${PG_DATABASE} \
  -Dpg.auth.mode=${PG_AUTH_MODE} \
  -Dkafka.enable=${KAFKA_ENABLE} \
  -Dkafka.bind.address=${KAFKA_BIND_ADDRESS} \
  -Dkafka.bind.port=${KAFKA_BIND_PORT} \
  -Dkafka.default.database=${KAFKA_DEFAULT_DATABASE} \
  ${SERVER_ID:+-Dserver.id=$SERVER_ID} \
  ${TABLES:+-Dhbase.compat.tables=$TABLES} \
  -jar /app/fluss-cape.jar"]
