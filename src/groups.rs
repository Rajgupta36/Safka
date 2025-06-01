use std::collections::{HashMap, HashSet};

pub struct ConsumerGroupManager {
    // map for group and consumer
    groups: HashMap<String, HashSet<String>>,
    // for offset and partition in group
    offsets: HashMap<String, HashMap<usize, usize>>,
}

impl ConsumerGroupManager {
    pub fn new() -> Self {
        Self {
            groups: HashMap::new(),
            offsets: HashMap::new(),
        }
    }

    /// Join a consumer group, registering a consumer ID if not already present.
    pub fn join_group(&mut self, group_id: &str, consumer_id: String) {
        self.groups
            .entry(group_id.to_string())
            .or_default()
            .insert(consumer_id);
    }

    /// Returns current offset for a group+partition. Defaults to 0.
    pub fn get_offset(&self, group_id: &str, partition_id: usize) -> usize {
        self.offsets
            .get(group_id)
            .and_then(|m| m.get(&partition_id).copied())
            .unwrap_or(0)
    }

    /// Commits an offset for a group+partition.
    pub fn commit_offset(&mut self, group_id: &str, partition_id: usize, offset: usize) {
        self.offsets
            .entry(group_id.to_string())
            .or_default()
            .insert(partition_id, offset);
    }

    /// If offset not yet initialized, sets it based on current buffer len or 0.
    pub fn get_or_init_offset(
        &mut self,
        group_id: &str,
        partition_id: usize,
        start_from_latest: bool,
        current_len: usize,
    ) -> usize {
        let group_offsets = self.offsets.entry(group_id.to_string()).or_default();
        *group_offsets.entry(partition_id).or_insert_with(|| {
            if start_from_latest {
                current_len
            } else {
                0
            }
        })
    }
}
