FROM golang:1.25-alpine3.23 AS base

FROM base AS builder
WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY animey_source_processor.go ./
RUN go build -tags source_processor -o animey_source_processor

FROM base AS runner
WORKDIR /app
RUN apk add --no-cache ffmpeg
COPY --from=builder /app/animey_source_processor ./
CMD ["./animey_source_processor"]