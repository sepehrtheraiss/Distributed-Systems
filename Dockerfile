# Using lighter version of golang.
FROM golang:alpine

# Set the working directory.
WORKDIR /app
ADD . /app
ADD vendor/ $GOPATH/src/

# Build the golang source.
RUN go build -o server

# Environment variables.
ENV GOSSIP "4"

# Execute the server.
CMD ["./server"]
