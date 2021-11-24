# Gouge

Gouge is a distributed SQL server built on ChiselStore and gRPC.

## Getting Started

Start a cluster of three nodes:

```
cargo run --example gouged -- --id 1 --peers 2 3
cargo run --example gouged -- --id 2 --peers 1 3
cargo run --example gouged -- --id 3 --peers 1 2
```

Then run some SQL commands:

```
cargo run --example gouge
```
