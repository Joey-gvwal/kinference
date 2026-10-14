fn main() -> Result<(), Box<dyn std::error::Error>> {
    // `KINFERENCE_VERSION` overrides `CARGO_PKG_VERSION` (see
    // `src/version.rs`); the justfile sets it from the image tag. Rebuild
    // when it changes, or a new tag would keep the old version.
    println!("cargo:rerun-if-env-changed=KINFERENCE_VERSION");

    // The vendored `protoc`, so the build doesn't depend on a system
    // protobuf compiler.
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    let protoc_include = protoc_bin_vendored::include_path()?;
    // SAFETY: build.rs runs single-threaded before any user code;
    // there are no other threads to race with on this env var.
    unsafe {
        std::env::set_var("PROTOC", &protoc);
        std::env::set_var("PROTOC_INCLUDE", &protoc_include);
    }

    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(
            &["proto/kinference.proto"],
            &[
                "proto",
                protoc_include
                    .to_str()
                    .expect("protoc include path is utf-8"),
            ],
        )?;
    Ok(())
}
