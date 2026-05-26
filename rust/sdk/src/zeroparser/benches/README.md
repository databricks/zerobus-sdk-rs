## Reproducing C++ numbers

The plot's C++ bars are pinned constants in `e2e/benches/bench_plot.rs`,
measured by an out-of-tree harness so this crate keeps no C++ source or
build dependency. To re-measure on your machine:

1. Dump the exact encoded bytes the Rust benches see:
   ```bash
   ZEROPARSER_CPP_DUMP_DIR=/tmp/zeroparser-cpp-bench/data \
     cargo bench --bench bench_plot
   ```
2. In a scratch directory outside this repo, drop the bench `.proto` files
   alongside a small driver that:
   - generates C++ stubs with the same `protoc` version as your libprotobuf
     (`protoc --cpp_out=.`);
   - parses each `<label>@<bytes>B.bin` file `N` times, walking every field
     once via both `Reflection`/`GetXxx` (apples-to-apples with
     `prost-reflect` / Zeroparser) and generated accessors (apples-to-apples
     with `prost`);
   - prints `<scenario>,cpp_reflect,<MB/s>` / `<scenario>,cpp_typed,<MB/s>`.
3. Average a few runs and update the `cpp_baselines()` map in
   `e2e/benches/bench_plot.rs`, then re-run `cargo bench --bench bench_plot`.
