FROM golang:1.22 AS build

# `boilerplate` should be replaced with your project name
WORKDIR /go/src/ftso-data-sources

# Copy all the Code and stuff to compile everything
COPY . .
RUN go mod download

# Builds the application as a staticly linked one, to allow it to run on alpine
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o ftso-data-sources ./

# Moving the binary to the 'final Image' to make it smaller
FROM alpine:latest as release
WORKDIR /app

# `boilerplate` should be replaced here as well
COPY --from=build /go/src/ftso-data-sources/ftso-data-sources .

# Add packages
RUN apk -U upgrade \
    && apk add --no-cache dumb-init ca-certificates tzdata \
    && chmod +x /app/ftso-data-sources


ENTRYPOINT ["/usr/bin/dumb-init", "--"]

CMD [ "/app/ftso-data-sources" ]
