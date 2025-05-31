use crate::deserializer::IncomingMessage;
use crate::partitionManager::PartitionManager;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{broadcast, Mutex};

pub async fn handle_client(
    mut stream: TcpStream,
    tx: broadcast::Sender<String>,
    partition_manager: Arc<Mutex<PartitionManager>>,
    rr_counter: Arc<AtomicUsize>,
) {
    let mut role_buf = [0u8; 1024];
    let n = stream.read(&mut role_buf).await.unwrap_or(0);
    let role = String::from_utf8_lossy(&role_buf[..n])
        .trim()
        .to_lowercase();

    match role.as_str() {
        "producer" => {
            let mut buf = [0u8; 1024];
            while let Ok(n) = stream.read(&mut buf).await {
                if n == 0 {
                    break;
                }
                let parsed = serde_json::from_slice::<IncomingMessage>(&buf[..n]);
                let total_partitions = partition_manager.lock().await.total_partitions();
                match parsed {
                    Ok(msg) => {
                        let partition_id = msg.partitionId.unwrap_or_else(|| {
                            let id = rr_counter.fetch_add(1, Ordering::SeqCst) % total_partitions;
                            id
                        });
                        println!("current assigned partition is {}", partition_id);
                        let partition = partition_manager.lock().await.get_partition(partition_id);

                        if partition.is_none() {
                            let _ = stream
                                .write_all(
                                    format!("Partition {} does not exist\n", partition_id)
                                        .as_bytes(),
                                )
                                .await;
                            continue;
                        }

                        partition
                            .as_ref()
                            .unwrap()
                            .lock()
                            .unwrap()
                            .push(msg.message.clone());
                        println!(
                            "circular buffer is {:?}",
                            partition.as_ref().unwrap().lock().unwrap().get_all()
                        );
                        let _ = stream
                            .write_all(
                                format!("Message added to partition {}\n", partition_id).as_bytes(),
                            )
                            .await;
                    }
                    Err(e) => {
                        eprintln!("Failed to parse message: {:?}", e);
                        let _ = stream
                            .write_all(b"Please provide the data in correct format\n")
                            .await;
                        continue;
                    }
                }
            }
        }
        "consumer" => {
            print!("hiii consumer\n");
        }
        _ => {
            let _ = stream.write_all(b"Invalid role\n").await;
        }
    }
}
