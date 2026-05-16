### market-wide-anomaly-tracker

```
{"action":"sub","params":"A.TSLA"}
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
{"action":"sub","symbols":["NVDA","AAPL","MSFT","GOOGL","AMZN","META","TSLA","AVGO","ORCL","ADBE","AMD","INTC","NFLX","CRM","CSCO","QCOM","TXN","MU","AMAT","PYPL","JPM","BAC","GS","MS","V","MA","WMT","COST","TGT","DIS","BA","CAT","GE","MMM","XOM","CVX","PFE","JNJ","UNH","ABBV","SPY","QQQ","IWM","DIA","VIX","SOXL","TQQQ","SQ","COIN","HOOD"]}
```

**Timestream**
```
{"action":"sub","params":"TIMESTREAM"}
```
```
{"action":"unsub","params":"TIMESTREAM"}
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
docker build . -t replayengine &&
docker run --network host -e MASSIVE_API_KEY=$MASSIVE_API_KEY replayengine:latest
```

#### Docker Compose
```
docker compose up
docker compose up --build
```