FROM golang:1.25-alpine AS build

WORKDIR /go/src/ftso-data-sources

RUN apk add --no-cache \
    build-base \
    pkgconf \
    git \
    ca-certificates \
    tzdata \
    zeromq \
    zeromq-dev \
    czmq-static \
    libzmq-static \
    libsodium-static

COPY go.mod go.sum ./
RUN go mod download

COPY . .

ENV CGO_ENABLED=1 \
    GOOS=linux \
    GOARCH=amd64

RUN go build \
    -tags musl \
    -ldflags="-w -s -linkmode external -extldflags '-static -lstdc++ -lm -lsodium'" \
    -o ftso-data-sources .

# --- Release Stage ---
FROM scratch AS release

WORKDIR /app

COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=build /usr/share/zoneinfo /usr/share/zoneinfo

COPY --from=build /go/src/ftso-data-sources/ftso-data-sources /ftso-data-sources

ENTRYPOINT ["/ftso-data-sources"]