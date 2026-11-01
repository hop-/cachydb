# syntax=docker/dockerfile:1

# Official cachydb image: a static Go binary on distroless, running as the
# non-root user 65532 with its data in the /data volume. Configure it with
# environment variables (TRANSPORT, PORT, DB_NAME, ...); see DOCKERHUB.md.
#
# Build:   docker build --build-arg VERSION=v0.0.4 -t cachydb .
# Release: .github/workflows/docker.yml builds linux/amd64 and linux/arm64
#          and pushes hovhannespalyan/cachydb on v* tags.

# The build stage runs on the builder's own platform and cross-compiles
# for TARGETOS/TARGETARCH (CGO_ENABLED=0), so multi-arch builds need no
# emulation. Base images are pinned by digest; the comments name the tags.

# golang:1.27 (Go 1.27.3; 1.25 is out of security support), multi-arch index digest. go.mod keeps 1.25.5 as the minimum.
FROM --platform=$BUILDPLATFORM golang:1.27@sha256:2920bb124f1a2e12a1d39f0cab756ae0068441ee1da7655dec7f5e374944095b AS build
ARG TARGETOS
ARG TARGETARCH
ARG VERSION=dev
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -ldflags "-s -w -X github.com/hop-/cachydb/internal/cmd.Version=${VERSION}" -o /out/cachydb . \
 && mkdir -p /out/data

# gcr.io/distroless/static-debian12:nonroot, multi-arch index digest
FROM gcr.io/distroless/static-debian12:nonroot@sha256:afa5c872c891853ca7fcf1f12c3edb23f7eeef36189728842dd51042ff57f7ab
COPY --from=build /out/cachydb /usr/local/bin/cachydb
COPY --from=build --chown=65532:65532 /out/data /data
# No flags in CMD: flags override environment variables, so the image is
# configured through the environment. Arguments after the image name are
# passed to cachydb (e.g. "version --json").
ENV TRANSPORT=http PORT=7601 ROOT_DIR=/data DB_NAME=main
EXPOSE 7601
VOLUME ["/data"]
USER 65532:65532
# Probes http://127.0.0.1:$PORT/healthz; exits 0 without probing when
# TRANSPORT=stdio.
HEALTHCHECK --interval=10s --timeout=5s --start-period=10s --retries=3 \
  CMD ["/usr/local/bin/cachydb", "healthcheck"]
ENTRYPOINT ["/usr/local/bin/cachydb"]
