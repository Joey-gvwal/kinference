//! The router: the service, scoring, the picker, the body store, the
//! tokenizer, observations and gRPC.

pub mod body_store;
pub mod error;
pub mod grpc;
pub mod metrics;
pub mod observations;
pub mod pick_body;
pub mod picker;
pub mod proto;
pub mod service;
pub mod tokenizer;
