use crate::deserializer::IncomingMessage;
use crate::groups::ConsumerGroupManager;
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
    consumer_group_manager: Arc<Mutex<ConsumerGroupManager>>,
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
            let mut buf = [0u8; 1024];
            let n = stream.read(&mut buf).await.unwrap_or(0);
            if n == 0 {
                return;
            }

            let ids = String::from_utf8_lossy(&buf[..n]);
            let parts: Vec<&str> = ids.trim().split(':').collect();

            if parts.len() < 2 {
                let _ = stream
                    .write_all(b"Invalid format. Use group_id:consumer_id:offset\n")
                    .await;
                return;
            }

            let group_id = parts[0];
            let consumer_id = parts[1];
            let start_from_latest = parts.get(2).map(|v| *v == "true").unwrap_or(false);

            {
                let mut cg_lock = consumer_group_manager.lock().await;
                if let Err(err_msg) = cg_lock.join_group(group_id, consumer_id.to_string()) {
                    let _ = stream
                        .write_all(format!("Error: {}\n", err_msg).as_bytes())
                        .await;
                    return;
                }
            }

            loop {
                // Get current assignments
                let assigned_partitions = {
                    let cg_lock = consumer_group_manager.lock().await;
                    cg_lock.get_assignments(group_id, consumer_id)
                };

                if assigned_partitions.is_empty() {
                    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
                    continue;
                }

                let mut messages_found = false;

                for &partition_id in &assigned_partitions {
                    // Get partition buffer
                    let buffer = {
                        let pm = partition_manager.lock().await;
                        match pm.get_partition(partition_id) {
                            Some(p) => p,
                            None => continue,
                        }
                    };

                    let current_len = buffer.lock().unwrap().get_all().len();

                    let offset = {
                        let mut cg = consumer_group_manager.lock().await;
                        cg.get_or_init_offset(
                            group_id,
                            partition_id,
                            start_from_latest,
                            current_len,
                        )
                    };

                    let msgs = buffer.lock().unwrap().get_all();

                    if offset < msgs.len() {
                        messages_found = true;
                        let msg = &msgs[offset];
                        if stream
                            .write_all(format!("[partition {}] {}\n", partition_id, msg).as_bytes())
                            .await
                            .is_err()
                        {
                            let mut cg = consumer_group_manager.lock().await;
                            cg.leave_group(group_id, consumer_id);
                            return;
                        }

                        // Commit offset
                        let mut cg = consumer_group_manager.lock().await;
                        cg.commit_offset(group_id, partition_id, offset + 1);
                    }
                }

                // if no messages
                if !messages_found {
                    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
                }
            }
        }

        _ => {
            let _ = stream.write_all(b"Invalid role\n").await;
        }
    }
}
