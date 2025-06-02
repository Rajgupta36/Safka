use crate::groups::ConsumerGroupManager;
use crate::handler::handle_client;
use crate::partitionManager::PartitionManager;

use std::sync::{atomic::AtomicUsize, Arc};
use tokio::sync::broadcast;
use tokio::{net::TcpListener, sync::Mutex};

pub async fn start_server() -> anyhow::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:9000").await?;
    println!("Listening on 127.0.0.1:9000");

    let (tx, _rx) = broadcast::channel::<String>(100);
    //by default we providing 2 partitions
    let partition_manager = Arc::new(Mutex::new(PartitionManager::new(2, 100)));
    let consumer_group_manager = Arc::new(Mutex::new(ConsumerGroupManager::new(
        partition_manager.lock().await.total_partitions(),
    )));

    let rr_counter = Arc::new(AtomicUsize::new(0));

    loop {
        let (stream, addr) = listener.accept().await?;
        println!("Connected: {}", addr);

        let tx = tx.clone();
        let partition_manager = partition_manager.clone();
        let consumer_group_manager = consumer_group_manager.clone();
        let rr_counter = rr_counter.clone();

        tokio::spawn(async move {
            handle_client(
                stream,
                tx,
                partition_manager,
                rr_counter,
                consumer_group_manager,
            )
            .await;
        });
    }
}
