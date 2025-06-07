use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::circularBuffer::CircularBuffer;

pub type PartitionId = usize;

pub struct PartitionManager {
    pub partitions: HashMap<PartitionId, Arc<Mutex<CircularBuffer>>>,
}

impl PartitionManager {
    pub fn new(total_partitions: usize, capacity: usize) -> Self {
        let mut partitions = HashMap::new();
        for id in 0..total_partitions {
            partitions.insert(id, Arc::new(Mutex::new(CircularBuffer::new(capacity))));
        }
        Self { partitions }
    }

    pub fn get_partition(&self, id: PartitionId) -> Option<Arc<Mutex<CircularBuffer>>> {
        self.partitions.get(&id).cloned()
    }
    pub fn total_partitions(&self) -> usize {
        self.partitions.len()
    }
}
