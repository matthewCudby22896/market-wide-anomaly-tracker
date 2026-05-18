#!/bin/bash
exec > /var/log/user-data.log 2>&1
set -x

cd ~
mkdir -p /app
cd /app

cat << EOF > compose.yaml
services:
  timescaledb:
    image: timescale/timescaledb:latest-pg18
    ports:
     - "6543:5432" # external:internal
    environment:
     - POSTGRES_PASSWORD=${POSTGRES_PASSWORD}

  replayengine:
    build: .
    image: mcudby22896/replayengine:latest
    ports:
     - "8080:8080" # external:internal
    environment:
     - POSTGRES_DB=${POSTGRES_DB}
     - POSTGRES_PASSWORD=${POSTGRES_PASSWORD}
     - MASSIVE_API_KEY=${MASSIVE_API_KEY}
    command: ["replayengine", "-db-url", "timescaledb:5432", "-port", "8080"]
EOF

docker compose up -d --pull always
