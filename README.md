### market-wide-anomaly-tracker

##### Running `replayengine` locally (option 1)

1. Launch TimescaleDB

```bash
sudo docker rm -f timescaledb
docker run -d --name timescaledb \
    -p 6543:5432 \
    -e POSTGRES_PASSWORD=password \
    timescale/timescaledb-ha:pg18
```

2. Run the replayengine service
```bash
docker build . -t replayengine &&
docker run --network host -e MASSIVE_API_KEY=$MASSIVE_API_KEY replayengine:latest
```

##### Running `replayengine` via Docker Compose (option 2)

```bash
docker compose up
docker compose up --build
```

##### Connecting to local `replayengine`

Connect via `websocat`
```bash
websocat -v ws://localhost:8080/ws
```
Sub / Unsub to symbol
```json
{"action":"sub","params":"A.TSLA"}
{"action":"sub","params":"AM.TSLA"}
{"action":"unsub","params":"A.TSLA"}
{"action":"unsub","params":"AM.TSLA"}
```
```json
{"action":"sub","params":"A.NVDA"}
{"action":"sub","params":"AM.NVDA"}
{"action":"unsub","params":"A.NVDA"}
{"action":"unsub","params":"AM.NVDA"}
```

Sub / Unsub to timestream updates
```json
{"action":"sub","params":"TIMESTREAM"}
{"action":"unsub","params":"TIMESTREAM"}
```

##### Useful Commands

Clear all docker containers
```bash
# Remove all docker containers
sudo docker rm -f $(docker ps -aq)

# List docker images
docker image ls

# show pid of process running on port 8080
lsof -i :8080  
```
