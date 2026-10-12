//! The `kinference` server binary. Everything lives in the library; see
//! [`kinference::server::run`].

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    kinference::server::run().await
}
