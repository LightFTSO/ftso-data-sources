FROM golang:1.22 AS build

# `boilerplate` should be replaced with your project name
WORKDIR /go/src/ftso-data-sources

# Copy all the Code and stuff to compile everything
COPY . .
RUN go mod download

# Builds the application as a staticly linked one, to allow it to run on alpine
RUN GOOS=linux go build -o ftso-data-sources ./

# Moving the binary to the 'final Image' to make it smaller
FROM ubuntu:latest as release
WORKDIR /app

# `boilerplate` should be replaced here as well
COPY --from=build /go/src/ftso-data-sources/ftso-data-sources .

# Add packages
RUN apt update && apt install -y \
    ca-certificates tzdata && \
    chmod +x /app/ftso-data-sources

ENTRYPOINT [ "/app/ftso-data-sources" ]
