//! The generated protobuf/tonic types for `kinference.v1`.
//!
//! Nothing here is hand-written: `build.rs` compiles `proto/kinference.proto` with
//! `tonic_build`, and this module is just where the result is mounted.
//!
//! It sits at the crate root rather than inside `crate::grpc` — where it used
//! to live — because the generated code is shared vocabulary, not part of the
//! `Picker` handler. Two subsystems implement a service from it
//! (`crate::grpc` the `Picker`, `crate::token_review` the `TokenReview`),
//! and `crate::mock` implements a second `Picker`. Reaching those through
//! `crate::grpc` made every one of
//! them depend on a 668-line file they otherwise have no interest in, and put
//! `grpc.rs` in the edit path of subsystems that never touch it.
//!
//! The mount point is invisible to the wire format and to the build: the
//! service names in `proto/kinference.proto` are what callers see, and
//! `include_proto!` expands the same bytes wherever it is invoked.

tonic::include_proto!("kinference.v1");
