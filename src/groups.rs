use std::collections::{HashMap, HashSet};

pub struct ConsumerGroupManager {
    // groups and consumers
    groups: HashMap<String, HashSet<String>>,

    // offset for each group and partition
    offsets: HashMap<String, HashMap<usize, usize>>,

    // assignments of partitions to consumers in each group
    assignments: HashMap<String, HashMap<String, Vec<usize>>>,

    // total number of partitions for the topic (global)
    partition_count: usize,
}

impl ConsumerGroupManager {
    pub fn new(partition_count: usize) -> Self {
        Self {
            groups: HashMap::new(),
            offsets: HashMap::new(),
            assignments: HashMap::new(),
            partition_count,
        }
    }

    /// A consumer joins a group
    pub fn join_group(&mut self, group_id: &str, consumer_id: String) -> Result<(), String> {
        let group = self.groups.entry(group_id.to_string()).or_default();
        if group.contains(&consumer_id) {
            Err(format!(
                "Consumer ID '{}' already exists in group '{}'",
                consumer_id, group_id
            ))
        } else {
            group.insert(consumer_id);
            self.rebalance(group_id);
            Ok(())
        }
    }

    /// A consumer leaves a group
    pub fn leave_group(&mut self, group_id: &str, consumer_id: &str) {
        if let Some(group) = self.groups.get_mut(group_id) {
            group.remove(consumer_id);
            if group.is_empty() {
                self.groups.remove(group_id);
                self.assignments.remove(group_id);
            } else {
                self.rebalance(group_id);
            }
        }
    }

    fn rebalance(&mut self, group_id: &str) {
        let Some(consumers) = self.groups.get(group_id) else {
            return;
        };

        let mut consumer_list: Vec<&String> = consumers.iter().collect();
        consumer_list.sort(); // deterministic ordering

        let mut new_assignment: HashMap<String, Vec<usize>> = HashMap::new();

        for (i, partition_id) in (0..self.partition_count).enumerate() {
            let consumer = consumer_list[i % consumer_list.len()];
            new_assignment
                .entry(consumer.clone())
                .or_default()
                .push(partition_id);
        }

        self.assignments
            .insert(group_id.to_string(), new_assignment.clone());

        println!("Rebalanced group '{group_id}':");
        for (consumer, partitions) in new_assignment {
            println!("  Consumer {consumer} => Partitions {:?}", partitions);
        }
    }

    pub fn get_assignments(&self, group_id: &str, consumer_id: &str) -> Vec<usize> {
        self.assignments
            .get(group_id)
            .and_then(|m| m.get(consumer_id).cloned())
            .unwrap_or_default()
    }

    pub fn commit_offset(&mut self, group_id: &str, partition_id: usize, offset: usize) {
        self.offsets
            .entry(group_id.to_string())
            .or_default()
            .insert(partition_id, offset);
    }

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
