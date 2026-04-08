FROM golang:1.24-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w -X main.Version=${VERSION}" -o kubemq-keda-scaler .

FROM gcr.io/distroless/static:nonroot
COPY --from=builder /app/kubemq-keda-scaler /
EXPOSE 9090
ENTRYPOINT ["/kubemq-keda-scaler"]
