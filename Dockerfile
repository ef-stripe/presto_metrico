FROM golang:1.12.5

WORKDIR /app
COPY . .

# TODO: look into fmt and vet
RUN go build -o presto_metrico \
        && go test -v

ENTRYPOINT "/app/presto_metrico"
