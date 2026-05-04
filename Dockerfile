FROM golang:1.25.9-alpine AS builder

WORKDIR /usr/src/app

# pre-cop/cache go.mod for pre-downloading dependencies and only redownloading them in
# subsequent builds if they change
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# CGO_ENABLE : >reddit: With CGO_ENABLED=0 you get a statically linked binary so it will
# run without any external dependencies.
# GOOS: Go Operating System. Tells the Go compiler which OS the final binary should be 
# built for
RUN CGO_ENABLED=0 GOOS=linux
RUN go build -v -o /usr/local/bin/replayengine ./components/cmd/replay_engine/main.go

FROM alpine:latest

COPY --from=builder /usr/local/bin/replayengine /usr/local/bin/replayengine

# default, overwritten by command in compose.yaml 
CMD ["replayengine", "-db-url", "localhost:6543", "-port", "8080"]

