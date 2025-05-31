use crate::cicularBuffer::CircularBuffer;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{broadcast, Mutex};

pub async fn handle_client(
    mut stream: TcpStream,
    tx: broadcast::Sender<String>,
    buffer: Arc<Mutex<CircularBuffer>>,
) {
    let mut role_buf = [0u8; 1024];
    let n = stream.read(&mut role_buf).await.unwrap_or(0);
    let role = String::from_utf8_lossy(&role_buf[..n])
        .trim()
        .to_lowercase();

    let mut rx = tx.subscribe();

    match role.as_str() {
        "producer" => {
            let mut buf = [0u8; 1024];
            while let Ok(n) = stream.read(&mut buf).await {
                if n == 0 {
                    break;
                }
                let msg = String::from_utf8_lossy(&buf[..n]).to_string();
                {
                    let mut buf_lock = buffer.lock().await;
                    buf_lock.push(msg.clone());
                }
                let _ = tx.send(msg);
            }
        }
        "consumer" => {
            let previous_msgs = {
                let buf_lock = buffer.lock().await;
                buf_lock.get_all()
            };

            for msg in previous_msgs {
                let _ = stream.write_all(msg.as_bytes()).await;
                let _ = stream.write_all(b"\n").await;
            }

            while let Ok(msg) = rx.recv().await {
                let _ = stream.write_all(msg.as_bytes()).await;
                let _ = stream.write_all(b"\n").await;
            }
        }
        _ => {
            let _ = stream.write_all(b"Invalid role\n").await;
        }
    }
}
