fn main() -> Result<(), Box<dyn std::error::Error>> {
    // `KINFERENCE_VERSION` overrides `CARGO_PKG_VERSION` for the
    // version string surfaced by `--version`, the dashboard, and
    // `/api/snapshot`'s `version` field. The Makefile sets it from
    // `TAG=...` so released binaries advertise the same identity as
    // their container tag. Without this directive cargo wouldn't
    // recompile when only the env var changed, and tag updates would
    // silently keep the old version baked in.
    println!("cargo:rerun-if-env-changed=KINFERENCE_VERSION");

    // Use the vendored `protoc` rather than whatever happens to be on PATH, so
    // the build does not depend on a system protobuf-compiler being installed
    // or being a compatible version. (It also brought in the bundled
    // `google/protobuf/*.proto` well-known types, which the since-removed
    // `envoy_ext_proc.proto` imported; `kinference.proto` imports nothing, so that
    // part is no longer load-bearing.)
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
