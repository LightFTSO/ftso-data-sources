# Building the binary of the App
FROM golang:1.22 AS build

# `boilerplate` should be replaced with your project name
WORKDIR /app

# Copy all the Code and stuff to compile everything
COPY . .

# Downloads all the dependencies in advance (could be left out, but it's more clear this way)
RUN go mod download

# Builds the application as a staticly linked one, to allow it to run on alpine
RUN GOOS=linux go build -a -o /app/ftso-data-sources

EXPOSE 5500

ENTRYPOINT ["/app/ftso-data-sources", "--"]