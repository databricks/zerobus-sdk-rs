use std::sync::atomic::{AtomicI64, Ordering};

/// Offset ID type.
pub type OffsetId = i64;

/// This struct is used to generate offset IDs for ingested records.
/// It is used to ensure that the offset IDs are unique and monotonically increasing.
pub struct OffsetIdGenerator {
    last_offset_id: AtomicI64,
}

impl Default for OffsetIdGenerator {
    fn default() -> Self {
        Self {
            last_offset_id: AtomicI64::new(-1),
        }
    }
}

impl OffsetIdGenerator {
    /// Returns the next offset ID to send.
    pub fn next(&self) -> OffsetId {
        self.last_offset_id.fetch_add(1, Ordering::SeqCst) + 1
    }

    /// Returns the last offset ID sent.
    pub fn last(&self) -> Option<OffsetId> {
        let last_offset = self.last_offset_id.load(Ordering::SeqCst);
        if last_offset == -1 {
            None
        } else {
            Some(last_offset)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::thread;

    use crate::OffsetIdGenerator;

    #[test]
    fn test_initial_state() {
        let generator = OffsetIdGenerator::default();
        assert_eq!(generator.last(), None);
    }

    #[test]
    fn test_first_next_is_zero() {
        let generator = OffsetIdGenerator::default();
        assert_eq!(generator.next(), 0);
        assert_eq!(generator.last(), Some(0));
    }

    #[test]
    fn test_monotonic_sequence() {
        let generator = OffsetIdGenerator::default();

        assert_eq!(generator.next(), 0);
        assert_eq!(generator.next(), 1);
        assert_eq!(generator.next(), 2);
        assert_eq!(generator.next(), 3);
        //blblb
        assert_eq!(generator.last(), Some(3));
    }

    #[test]
    fn test_thread_safety() {
        let generator = Arc::new(OffsetIdGenerator::default());
        let mut handles = vec![];

        // Spawn 10 threads, each generating 100 IDs.
        for _ in 0..10 {
            let gen = generator.clone();
            let handle = thread::spawn(move || {
                let mut ids = vec![];
                for _ in 0..100 {
                    ids.push(gen.next());
                }
                ids
            });
            handles.push(handle);
        }

        // Collect all generated IDs.
        let mut all_ids = vec![];
        for handle in handles {
            all_ids.extend(handle.join().unwrap());
        }

        // Should have 1000 unique IDs from 0 to 999.
        all_ids.sort();
        assert_eq!(all_ids.len(), 1000);
        assert_eq!(all_ids[0], 0);
        assert_eq!(all_ids[999], 999);

        // Check no duplicates.
        for i in 0..999 {
            assert_eq!(all_ids[i] + 1, all_ids[i + 1]);
        }
    }
}
