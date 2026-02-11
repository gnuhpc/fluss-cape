# Stage 1: Build
FROM maven:3.9.6-eclipse-temurin-11 AS builder

WORKDIR /build

# Copy project files
COPY pom.xml .
COPY src ./src

# Build JAR with skipped tests (tests require running environment)
RUN mvn clean package -DskipTests

# Stage 2: Runtime
FROM eclipse-temurin:11-jre-jammy

LABEL maintainer="fluss-hbase-compat"
LABEL description="HBase Compatibility Layer for Apache Fluss"

# Create non-root user
RUN groupadd -r cape && useradd -r -g cape -m -d /app cape

# Install necessary packages (minimal)
RUN apt-get update && apt-get install -y \
    curl \
    netcat-traditional \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy the shaded JAR from builder stage
COPY --from=builder /build/target/fluss-cape-*.jar /app/fluss-cape.jar

# Chown directory to non-root user
RUN chown -R cape:cape /app

# Switch to non-root user
USER cape

# Expose ports
# 16020 - HBase RPC port
# 6379 - Redis port
# 5432 - PostgreSQL port
# 9092 - Kafka port
# 8081 - Health check HTTP port
EXPOSE 16020 6379 5432 9092 8081

# Set default environment variables
ENV FLUSS_BOOTSTRAP="localhost:9123" \
    ZK_QUORUM="localhost:2181" \
    BIND_ADDRESS="0.0.0.0" \
    BIND_PORT="16020" \
    HEALTH_PORT="8081" \
    TABLES="" \
    SERVER_ID="" \
    REDIS_ENABLE="true" \
    REDIS_BIND_ADDRESS="0.0.0.0" \
    REDIS_BIND_PORT="6379" \
    REDIS_SHARDING_ENABLED="true" \
    REDIS_SHARDING_NUM_SHARDS="16" \
    PG_ENABLE="true" \
    PG_BIND_ADDRESS="0.0.0.0" \
    PG_BIND_PORT="5432" \
    PG_DATABASE="default" \
    PG_AUTH_MODE="trust" \
    KAFKA_ENABLE="true" \
    KAFKA_BIND_ADDRESS="0.0.0.0" \
    KAFKA_BIND_PORT="9092" \
    KAFKA_DEFAULT_DATABASE="default"

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
  CMD curl -f http://localhost:${HEALTH_PORT}/health || exit 1

# Run the application
ENTRYPOINT ["sh", "-c", "java -Xmx2g -Xms1g \
  -Dlog4j.configurationFile=classpath:log4j2.xml \
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
