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
COPY target/fluss-hbase-compat-1.0.0-SNAPSHOT.jar /app/fluss-hbase-compat.jar

# Expose ports
# 16020 - HBase RPC port
# 8080 - Health check HTTP port
EXPOSE 16020 8080

# Set default environment variables
ENV FLUSS_BOOTSTRAP="localhost:9123"
ENV ZK_QUORUM="localhost:2181"
ENV BIND_ADDRESS="0.0.0.0"
ENV BIND_PORT="16020"
ENV HEALTH_PORT="8080"
ENV TABLES=""
ENV SERVER_ID=""

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
  ${SERVER_ID:+-Dserver.id=$SERVER_ID} \
  ${TABLES:+-Dhbase.compat.tables=$TABLES} \
  -jar /app/fluss-hbase-compat.jar"]
