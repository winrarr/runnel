mod bootstrap;
mod connection;
mod dispatch;
mod lifecycle;
mod observability;
mod protocol;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "runnel=info".into()),
        )
        .init();

    bootstrap::run().await
}
