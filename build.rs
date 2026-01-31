fn main() {
    tonic_build::configure()
        .compile(&["proto/kv.proto"], &["proto/"])
        .expect("Failed to compile protos");
}
