
# Build the gobinary

FROM golang:1.22 as gobuild

RUN update-ca-certificates

WORKDIR /go/src/app

RUN --mount=type=cache,target=/go/pkg/mod/ \
    --mount=type=bind,source=go.sum,target=go.sum \
    --mount=type=bind,source=go.mod,target=go.mod \
    go mod download -x

COPY ./ ./

RUN CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build -v -ldflags "-s -w -linkmode external -extldflags -static" -o /go/bin/customerd ./cmds/customerd

FROM gcr.io/distroless/static

COPY --from=gobuild /go/bin/customerd /go/bin/customerd
EXPOSE 8080

ENTRYPOINT ["/go/bin/customerd"]
