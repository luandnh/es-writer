FROM golang:1.14-alpine

WORKDIR /go/src/es-writer/
COPY    . /go/src/es-writer/

RUN apk add --no-cache git
RUN CGO_ENABLED=0 GOOS=linux go build -o /app /go/src/es-writer/cmd/main.go

FROM alpine:3.11
RUN apk add --no-cache ca-certificates
COPY --from=0 /app /app
ENTRYPOINT ["/app"]
