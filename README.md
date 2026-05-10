### market-wide-anomaly-tracker

```
{"action":"sub","symbols":["TSLA"]}
```
```
{"action":"unsub","symbols":["TSLA"]}
```

```
{"action":"sub","symbols":["NVDA"]}
```
```
{"action":"unsub","symbols":["NVDA"]}
```

**Timestream**
```
{"action":"sub","symbols":["TIMESTREAM"]}
```
```
{"action":"unsub","symbols":["TIMESTREAM"]}
```


#### Cmd Line Websocket Connection
Start websocket connection
```
websocat -v ws://localhost:8080/ws
```

#### Local DB
Clear timescale db
```
sudo docker rm -f timescaledb
```
Clear all docker containers
```
sudo docker rm -f $(docker ps -aq)
```
Start timescale db
```
docker run -d --name timescaledb \
    -p 6543:5432 \
    -e POSTGRES_PASSWORD=password \
    timescale/timescaledb-ha:pg18
```

#### Docker Commands
General cmds
```
docker image ls
```
Build & run replayengine image
```
docker build . -t replayengine
docker run --network host -e MASSIVE_API_KEY=$MASSIVE_API_KEY replayengine:latest
```

#### Docker Compose
```
docker compose up
docker compose up --build
```

#### Symbol Thread Redesign

Idea: Instead of having each symbol-thread load the entire series for simulation date
into memory, use 2 fixed-size buffers.

Write a new db access method that can use PGX to copy straight into these buffers.

During the simulaton, whilst one buffer is emptying have a side thread run to populate the now empty buffer.

When one is empty swap one out for another.

