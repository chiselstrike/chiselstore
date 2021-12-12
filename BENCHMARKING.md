## Benchmarking ChiselStore

First download the source code and build the benchmark:

```
mkdir benchmark && cd benchmark
git clone git@github.com:chiselstrike/chiselstore.git
git clone git@github.com:chiselstrike/ycsb-rs.git
cd ycsb-rs
cargo build --release
```

Now we're ready to benchmark!

Let's start with SQLite. First load some data:

```
./target/release/ycsb --database sqlite --workload workloads/workloadc.toml load
[OVERALL], ThreadCount, 1
[OVERALL], RunTime(ms), 3825
[OVERALL], Throughput(ops/sec), 261.437908496732
```

Then edit `workloads/workloadc.toml` to perform 1 M operations to make the benchmark run for a longer period of time:

```
operationcount = 1000000 
```

and then run the benchmark:

```
./target/release/ycsb --database sqlite --workload workloads/workloadc.toml --threads 20 run
[OVERALL], ThreadCount, 20
[OVERALL], RunTime(ms), 1288
[OVERALL], Throughput(ops/sec), 776397.5155279503 
```

Next, let's benchmark ChiselStore.

First, start two ChiselStore replicas in the `chiselstore` repository:

```
$ cargo run --release --example gouged -- --id 2 --peers 1 3
$ cargo run --release --example gouged -- --id 3 --peers 1 2
```

Then load data to the ChiselStore cluster (remember to change `operationscount` to 1000 in `workloadc.toml`):

```
./target/release/ycsb --database chiselstore --workload workloads/workloadc.toml load
[OVERALL], ThreadCount, 1
[OVERALL], RunTime(ms), 7048
[OVERALL], Throughput(ops/sec), 141.88422247446084 
```

Finally, bump `operationcount` to `1000000`, and run the benchmark:

```
./target/release/ycsb --database chiselstore --workload workloads/workloadc.toml run --threads 20
[OVERALL], ThreadCount, 20
[OVERALL], RunTime(ms), 1369
[OVERALL], Throughput(ops/sec), 730460.1899196494
```
