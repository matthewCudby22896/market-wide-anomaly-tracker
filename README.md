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

```
{"action":"sub","symbols":["AMZN"]}
```
```
{"action":"unsub","symbols":["AMZN"]}
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

