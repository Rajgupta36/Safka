mod cicularBuffer;
mod handler;
mod server;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    server::start_server().await
}
