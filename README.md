# rpc_rust_distributed_system

A small RPC framework in Rust over QUIC, with mutual TLS. It is a learning and
research project: the transport is built and tested, and consensus is in progress.

## What works

- **Transport:** calls over QUIC (via `quinn`), many calls multiplexed on one
  connection, and a custom binary frame format with a 16-byte header.
- **Errors:** typed error codes modelled on gRPC status codes, sent back to the client.
- **Deadlines:** a timeout is sent with the request, so the server enforces it too.
- **Cancellation:** dropping a call tells the server to stop its work.
- **Security:** mutual TLS with a development certificate authority. The server
  rejects clients whose certificate is not signed by that authority.
- **Shutdown:** the server stops accepting work and waits up to 10 seconds for
  in-flight calls to finish.
- **Typed echo service:** a small example protocol built on the transport.

Known limits are listed in [KNOWN_LIMITS.md](KNOWN_LIMITS.md).

## Requirements

- Rust 1.85 or newer (the crate uses edition 2024).

## Run

```bash
cargo run
```

Prints `echo over QUIC + mTLS: sent "hi", received "hi": OK` and exits with 0.
Exits with an error if the echo reply is not `"hi"`.

## Test

```bash
cargo test
```

The 25 tests cover:
- the frame codec and echo message codec, including random-input property tests
  (`src/property_tests.rs`);
- the transport on real loopback connections: calls, errors, deadlines,
  cancellation, shutdown, concurrency, panicking handlers, malformed input, and
  TLS rejection (`src/transport_tests.rs`).

Lint with:

```bash
cargo clippy --all-targets
```

## Measured

Typed echo calls, one QUIC connection, sequential, over loopback. 500 warm-up
calls, then 10,000 measured calls with a 64-byte body. Release build.

```bash
cargo test --release bench_loopback -- --ignored --nocapture
```

| Run | Calls/s | Mean | p50 | p99 |
|---|---|---|---|---|
| 1 | 15,875 | 63 µs | 57 µs | 171 µs |
| 2 | 15,391 | 65 µs | 59 µs | 180 µs |
| 3 | 16,475 | 61 µs | 55 µs | 156 µs |

Median of three: **about 15,900 calls/s, p99 171 µs**.

Machine: Intel Core i5-12450H (8 cores, 12 threads), 16 GB RAM, Windows 11.

These numbers measure one client and one server in one process on loopback. They do
not include real network latency, and they are not a throughput test across many
connections.

## Layout

| File | Purpose |
|---|---|
| `src/frame.rs` | Frame format: 16-byte header, five frame kinds, codec |
| `src/codec.rs` | Field encoding helpers and the echo message |
| `src/rpc.rs` | Client, server, error codes, deadlines, cancellation |
| `src/tls.rs` | Development certificate authority and mutual-TLS configuration |
| `src/echo_service.rs` | Typed echo protocol: server dispatcher and client |
| `src/main.rs` | Demo: one echo call over QUIC with mutual TLS |

## Next

Leader election and log replication with Raft, then Byzantine fault tolerance.
This work is not started in this branch.
