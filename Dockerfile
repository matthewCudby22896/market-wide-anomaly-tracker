FROM golang:1.25

WORKDIR /usr/src/app

# pre-cop/cache go.mod for pre-downloading dependencies and only redownloading them in
# subsequent builds if they change
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN go build -v -o /usr/local/bin/replayengine ./components/cmd/replay_engine/main.go

# default, overwritten by command in compose.yaml 
CMD ["replayengine", "-db-url", "localhost:6543", "-port", "8080"]