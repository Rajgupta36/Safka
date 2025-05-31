pub struct CircularBuffer {
    buffer: Vec<Option<String>>,
    capacity: usize,
    start: usize,
    end: usize,
    size: usize,
}

impl CircularBuffer {
    pub fn new(capacity: usize) -> Self {
        CircularBuffer {
            buffer: vec![None; capacity],
            capacity,
            start: 0,
            end: 0,
            size: 0,
        }
    }

    pub fn push(&mut self, item: String) {
        if self.size == self.capacity {
            self.start = (self.start + 1) % self.capacity; // Overwrite the oldest item
        } else {
            self.size += 1;
        }
        self.buffer[self.end] = Some(item);
        self.end = (self.end + 1) % self.capacity;
    }

    pub fn get(&self, index: usize) -> Option<&String> {
        if index < self.size {
            let actual_index = (self.start + index) % self.capacity;
            self.buffer[actual_index].as_ref()
        } else {
            None
        }
    }

    pub fn get_all(&self) -> Vec<String> {
        let mut items = Vec::with_capacity(self.size);
        for i in 0..self.size {
            if let Some(item) = self.get(i) {
                items.push(item.clone());
            }
        }
        items
    }
}
