FROM brandoshmando/go:1.13.4-ticket-lock AS binary

ADD . /build

RUN apk add --no-cache --virtual .build-deps \
    gcc \
    musl-dev \
    openssl \
    zeromq-dev

WORKDIR /build
RUN CGO_ENABLED=1 go build cmd/server.go

FROM alpine:3.11.2
RUN apk add zeromq-dev

COPY --from=binary /build/server /app/
WORKDIR /app

EXPOSE 1337
ENTRYPOINT ["./server"]
