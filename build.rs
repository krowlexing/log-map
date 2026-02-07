fn main() {
    println!("cargo:rerun-if-changed=proto/kv.proto");
    tonic_prost_build::configure()
        .compile_protos(&["proto/kv.proto"], &["proto/"])
        .expect("Failed to compile proto/kv.proto");
}
