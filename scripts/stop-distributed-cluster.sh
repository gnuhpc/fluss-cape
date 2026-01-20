#!/bin/bash

set -e

PIDS_FILE="/tmp/fluss-cape-cluster.pids"

if [ ! -f "${PIDS_FILE}" ]; then
    echo "No cluster PIDs file found at ${PIDS_FILE}"
    echo "Attempting to find and kill any running servers..."
    pkill -9 -f "HBaseCompatServerLauncher" 2>/dev/null && echo "Killed running servers" || echo "No running servers found"
    exit 0
fi

echo "========================================"
echo "Stopping Fluss CAPE Cluster"
echo "========================================"

while read PID; do
    if ps -p ${PID} > /dev/null 2>&1; then
        echo "Stopping server (PID: ${PID})..."
        kill ${PID} 2>/dev/null || true
        sleep 2
        
        if ps -p ${PID} > /dev/null 2>&1; then
            echo "  Force killing PID ${PID}..."
            kill -9 ${PID} 2>/dev/null || true
        fi
        echo "  Server stopped"
    else
        echo "Server (PID: ${PID}) already stopped"
    fi
done < "${PIDS_FILE}"

rm -f "${PIDS_FILE}"

echo "========================================"
echo "Cluster stopped successfully!"
echo "========================================"
