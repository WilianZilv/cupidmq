# CupidMQ master

Rust crate `cupidmq` — matchmaker daemon (`cupidmq` bin) + [`CupidMQ::connect_producer`](src/client/mod.rs).

See [monorepo README](../README.md).

```bash
cargo run --release -- --config cupidmq.conf
cargo run --release --example cupidmq-producer -- --batch-mode --count 10
```
