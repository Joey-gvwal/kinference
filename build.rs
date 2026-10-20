fn main() {
    // `KINFERENCE_VERSION` overrides `CARGO_PKG_VERSION` (see
    // `src/version.rs`); the justfile sets it from the image tag. Rebuild
    // when it changes, or a new tag would keep the old version.
    println!("cargo:rerun-if-env-changed=KINFERENCE_VERSION");
}
