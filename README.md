### market-wide-anomaly-tracker

```
{"action":"subscribe","symbols":["TSLA"]}
```
```
{"action":"unsubscribe","symbols":["TSLA"]}
```

```
{"action":"subscribe","symbols":["NVDA"]}
```
```
{"action":"unsubscribe","symbols":["NVDA"]}
```

```
{"action":"subscribe","symbols":["AMZN"]}
```
```
{"action":"unsubscribe","symbols":["AMZN"]}
```

```
{"action":"subscribe","symbols":["TIMESTREAM"]}
```


### Cmd Line Websocket Connection
Start websocket connection
```
websocat -v ws://localhost:8080/ws
```

### Local DB
Clear timescale db
```
sudo docker rm -f timescaledb
```
Start timescale db
```
docker run -d --name timescaledb \
    -p 6543:5432 \
    -e POSTGRES_PASSWORD=password \
    timescale/timescaledb-ha:pg18
```


