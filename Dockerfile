FROM golang:1.22.1 as build

WORKDIR /app

COPY go.* /app/

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=ssh \
    go mod download

COPY . .

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go build $GO_ARGS -o /app/thebin

# Need glibc
FROM gcr.io/distroless/base

ENTRYPOINT ["/app/thebin"]
COPY --from=build /app/thebin /app/