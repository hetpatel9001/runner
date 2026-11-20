FROM cloudflare/cloudflared:latest@sha256:b94b96edf2c9c4360f8879c8580cfaa99077f19c849ec595ca2f711b27d4f344 AS tunnel
FROM golang:1.24-alpine@sha256:8bee1901f1e530bfb4a7850aa7a479d17ae3a18beb6e09064ed54cfd245b7191 AS etcd
WORKDIR /src
COPY control/raft/go.mod control/raft/go.sum ./
RUN go mod download
COPY control/raft/main.go ./
RUN CGO_ENABLED=0 go test ./... && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /runner-etcd-raft .

FROM rust:1.100-alpine@sha256:de1e0058543a81ed7b046629167db587c67a94bdf37cce14eb27f0ec83baaffb AS build
COPY --from=etcd /runner-etcd-raft /usr/local/bin/runner-etcd-raft
RUN apk add --no-cache build-base linux-headers
COPY --from=tunnel /usr/local/bin/cloudflared /usr/local/bin/cloudflared
# Remove upstream debug/symbol tables; retain the normal static executable.
RUN strip --strip-unneeded /usr/local/bin/cloudflared && /usr/local/bin/cloudflared --version
WORKDIR /src
COPY rust ./rust
COPY control/src/app.html ./control/src/app.html
ARG REVISION=development
WORKDIR /src/rust
RUN cargo test --locked --workspace
RUN RUNNER_BUILD_REVISION="$REVISION" cargo build --locked --release -p runner-control -p runner-agent -p runner-raft

FROM scratch AS binaries
COPY --from=build /usr/local/bin/cloudflared /cloudflared
COPY --from=etcd /runner-etcd-raft /runner-etcd-raft
COPY --from=build /src/rust/target/release/runner-control /runner-control
COPY --from=build /src/rust/target/release/runner-agent /runner-agent
COPY --from=build /src/rust/target/release/runner-raft /runner-raft
