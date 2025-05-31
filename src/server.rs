use crate::cicularBuffer::CircularBuffer;
use crate::handler::handle_client;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::{broadcast, Mutex};

pub async fn start_server() -> anyhow::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:9000").await?;
    println!("Listening on 127.0.0.1:9000");

    let (tx, _rx) = broadcast::channel::<String>(100);
    let buffer = Arc::new(Mutex::new(CircularBuffer::new(100)));

    loop {
        let (stream, addr) = listener.accept().await?;
        println!("Connected: {}", addr);

        let tx = tx.clone();
        let buffer = buffer.clone();

        tokio::spawn(async move {
            handle_client(stream, tx, buffer).await;
        });
    }
}
