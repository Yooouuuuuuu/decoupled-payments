# The Go implementation's image. Build from the repository root:
#   docker build -f go/Dockerfile -t decoupled-payments-go .
#   docker run --rm decoupled-payments-go validator --siteId=2

FROM golang:1.27 AS build
WORKDIR /src/go
COPY go/go.mod go/go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download
COPY go/ ./
# The tests compare the embedded copies of the schemas and the defaults with
# the originals, at ../schemas and ../config as in the repository.
COPY schemas/ /src/schemas/
COPY config/default.yaml /src/config/
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go test ./...
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/dp ./cmd/dp

FROM alpine:3.22
COPY --from=build /out/dp /usr/local/bin/dp
USER nobody
ENTRYPOINT ["dp"]
