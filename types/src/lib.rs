pub mod kv {
    tonic::include_proto!("kv");
}

pub use kv::Record;
