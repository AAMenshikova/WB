FROM golang:1.23 AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o main ./cmd;

FROM gcr.io/distroless/base-debian12

WORKDIR /app
COPY --from=builder /app/main .
COPY web/ ./web/

EXPOSE 8081
CMD ["/app/main"]