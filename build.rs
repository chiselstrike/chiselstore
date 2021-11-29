// TODO: Move to examples/ when the `OUT_DIR` not defined problem is solved.

fn main() -> std::io::Result<()> {
    let proto = "proto/proto.proto";
    tonic_build::compile_protos(proto)?;
    println!("cargo:rerun-if-changed={}", proto);
    Ok(())
}
