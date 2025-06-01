mod circularBuffer;
mod deserializer;
mod groups;
mod handler;
mod partitionManager;
mod server;
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    server::start_server().await
}
