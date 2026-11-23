ARG GO_IMAGE=golang:1.26.11@sha256:957952ae74dc010608717144399a59a2c6e82ea03d63be1c7ff6577c5a135f93
ARG NODE_IMAGE=node:22.23.3-alpine@sha256:0a7108bf6c7bf5de370ffb1a3ed6be93d405b43ff159f681a8d18c0e2bc2e402
ARG RUNTIME_IMAGE=alpine:3.23@sha256:85fe1e81d6758c208f3e1eed4338a1997e19d4be002d4dd32d3100c9a8c010a0

FROM --platform=$BUILDPLATFORM ${GO_IMAGE} AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download && go mod verify

COPY cmd ./cmd
COPY internal ./internal
COPY gen ./gen
ARG TARGETOS
ARG TARGETARCH
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -mod=readonly -trimpath -o /bin/server ./cmd/server

FROM ${NODE_IMAGE} AS frontend-build
WORKDIR /src
COPY package.json package-lock.json tsconfig.chat.json ./
COPY scripts ./scripts
COPY web ./web
RUN --mount=type=cache,target=/root/.npm npm ci
RUN npm run build:chat

FROM ${RUNTIME_IMAGE} AS final
RUN apk upgrade --no-cache && apk add --no-cache ca-certificates tzdata
ARG UID=10001
RUN adduser --disabled-password --gecos "" --home /nonexistent \
    --shell /sbin/nologin --no-create-home --uid "${UID}" appuser
COPY --from=frontend-build /src/web /web
RUN mkdir -p /web/uploads && chown -R appuser:appuser /web/uploads
RUN mkdir -p /var/lib/zchat/e2ee-blobs \
    && chown appuser:appuser /var/lib/zchat/e2ee-blobs \
    && chmod 0700 /var/lib/zchat/e2ee-blobs
COPY migrations /migrations
COPY --from=build /bin/server /bin/server
COPY LICENSE licensing.md /licenses/zchat/
COPY licenses /licenses/third-party/
USER appuser
EXPOSE 8080 50051
ENTRYPOINT ["/bin/server"]
