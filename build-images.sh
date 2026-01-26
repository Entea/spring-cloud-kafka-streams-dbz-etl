#!/bin/bash
set -e

# Build all modules with Maven
mvn clean package -DskipTests=true

# Build app image
docker build -t kafka-connect-etl/app:latest -f app/Dockerfile .

# Build transformer image
docker build -t kafka-connect-etl/transformer:latest -f transformer/Dockerfile .
