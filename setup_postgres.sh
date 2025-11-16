#!/bin/bash
################################################################################
# PostgreSQL Docker Setup Script
# Creates and initializes a PostgreSQL database in Docker
################################################################################

set -e  # Exit on error

# Configuration
CONTAINER_NAME="nyc-taxi-postgres"
POSTGRES_USER="postgres"
POSTGRES_PASSWORD="postgres"
POSTGRES_DB="nyc_taxi_db"
POSTGRES_PORT="5432"

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo "========================================================================"
echo "PostgreSQL Docker Setup"
echo "========================================================================"
echo ""

# Check if Docker is installed
if ! command -v docker &> /dev/null; then
    echo -e "${RED}Error: Docker is not installed${NC}"
    echo "Please install Docker first: https://docs.docker.com/get-docker/"
    exit 1
fi

# Check if container already exists
if docker ps -a --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
    echo -e "${BLUE}Container '${CONTAINER_NAME}' already exists${NC}"

    # Check if it's running
    if docker ps --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
        echo -e "${GREEN}✓ Container is already running${NC}"
    else
        echo "Starting existing container..."
        docker start ${CONTAINER_NAME}
        echo -e "${GREEN}✓ Container started${NC}"
    fi
else
    echo "Creating new PostgreSQL container..."
    docker run -d \
        --name ${CONTAINER_NAME} \
        -e POSTGRES_USER=${POSTGRES_USER} \
        -e POSTGRES_PASSWORD=${POSTGRES_PASSWORD} \
        -e POSTGRES_DB=${POSTGRES_DB} \
        -p ${POSTGRES_PORT}:5432 \
        postgres:15-alpine

    echo -e "${GREEN}✓ Container created and started${NC}"

    # Wait for PostgreSQL to be ready
    echo "Waiting for PostgreSQL to be ready..."
    sleep 5
fi

echo ""
echo "========================================================================"
echo "PostgreSQL Database Ready!"
echo "========================================================================"
echo ""
echo "Connection Details:"
echo "  Host:     localhost"
echo "  Port:     ${POSTGRES_PORT}"
echo "  Database: ${POSTGRES_DB}"
echo "  User:     ${POSTGRES_USER}"
echo "  Password: ${POSTGRES_PASSWORD}"
echo ""
echo "Connection String:"
echo "  postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@localhost:${POSTGRES_PORT}/${POSTGRES_DB}"
echo ""
echo "Useful Commands:"
echo "  Stop:    docker stop ${CONTAINER_NAME}"
echo "  Start:   docker start ${CONTAINER_NAME}"
echo "  Remove:  docker rm -f ${CONTAINER_NAME}"
echo "  Logs:    docker logs ${CONTAINER_NAME}"
echo "  Shell:   docker exec -it ${CONTAINER_NAME} psql -U ${POSTGRES_USER} -d ${POSTGRES_DB}"
echo ""
echo -e "${GREEN}Setup complete!${NC}"
