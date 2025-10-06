use std::collections::VecDeque;
use std::sync::Arc;

use thiserror::Error;
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};

#[derive(Debug, Error)]
pub enum LandingZoneError {
    #[error("Attempted to remove non-observed element")]
    RemovingNonObservedElement,
}

/// LandingZone has following properties:
/// 1. It keeps a queue of T.
/// 2. It provides a way to add to the queue.
/// 3. It provides a way to remove from the queue.
/// 4. It provides a way to read from the queue in
///    a blocking, non-consumable manner.
///
/// Remove from the queue only works if an element has been observed, i.e. that
/// observe() has been called against the given element.
///
/// Iteration can be restarted from the beginning.
struct LandingZoneState<T> {
    queue: VecDeque<T>,
    observed_items: VecDeque<T>,
}

pub struct LandingZone<T: Clone> {
    /// Synchronizes access to the landing zone.
    state: Arc<std::sync::Mutex<LandingZoneState<T>>>,
    /// Notifies waiting observe() calls when new items are added.
    new_item_notify: Arc<Notify>,
    /// Controls maximum number of inflight records.
    semaphore: Arc<Semaphore>,
    /// Tracks semaphore permits to release them when items are removed.
    permits: std::sync::Mutex<VecDeque<OwnedSemaphorePermit>>,
}

impl<T: Clone> LandingZone<T> {
    pub fn new(max_inflight_records: usize) -> Self {
        Self {
            state: Arc::new(std::sync::Mutex::new(LandingZoneState {
                queue: VecDeque::with_capacity(max_inflight_records),
                observed_items: VecDeque::with_capacity(max_inflight_records),
            })),
            new_item_notify: Arc::new(Notify::new()),
            semaphore: Arc::new(Semaphore::new(max_inflight_records)),
            permits: std::sync::Mutex::new(VecDeque::with_capacity(max_inflight_records)),
        }
    }

    /// Removes all items from the landing zone.
    pub fn remove_all(&self) -> Vec<T> {
        let mut state = self.state.lock().expect("Lock poisoned");

        let mut all_items = Vec::with_capacity(state.observed_items.len() + state.queue.len());
        all_items.extend(state.observed_items.drain(..));
        all_items.extend(state.queue.drain(..));

        let mut permits = self.permits.lock().expect("Lock poisoned");
        permits.clear();

        all_items
    }

    /// Adds an item to queue. Blocks if the semaphore has reached max_inflight_records.
    pub async fn add(&self, request: T) {
        let _permit = self
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .expect("Failed to acquire semaphore");
        let mut state = self.state.lock().expect("Lock poisoned");
        state.queue.push_back(request);
        self.permits
            .lock()
            .expect("Lock poisoned")
            .push_back(_permit);
        // Unblock one of the waiting observe() calls.
        self.new_item_notify.notify_one();
    }

    /// Removes an observed element. If element hasn't been observed, it will return an error.
    pub fn remove_observed(&self) -> Result<T, LandingZoneError> {
        let mut state = self.state.lock().expect("Lock poisoned");
        if let Some(item) = state.observed_items.pop_front() {
            self.permits.lock().expect("Lock poisoned").pop_front();
            Ok(item)
        } else {
            Err(LandingZoneError::RemovingNonObservedElement)
        }
    }

    /// Observes the next element in the queue by moving it to observed_items. Blocks if there are no items in the queue.
    pub async fn observe(&self) -> T {
        loop {
            let notified = self.new_item_notify.notified();
            {
                let mut state = self.state.lock().expect("Lock poisoned");
                if let Some(elem) = state.queue.pop_front() {
                    state.observed_items.push_back(elem.clone());
                    return elem;
                }
            }
            notified.await;
        }
    }

    /// Resets observed_items to the beginning by moving all observed items back to queue.
    pub fn reset_observe(&self) {
        let mut state = self.state.lock().expect("Lock poisoned");
        while let Some(observed_item) = state.observed_items.pop_back() {
            state.queue.push_front(observed_item);
        }
    }

    /// Returns true if observed_items is empty.
    pub fn is_observed_empty(&self) -> bool {
        let state = self.state.lock().expect("Lock poisoned");
        state.observed_items.is_empty()
    }

    /// Returns the number of items in the landing zone.
    pub fn len(&self) -> usize {
        let state = self.state.lock().expect("Lock poisoned");
        state.queue.len() + state.observed_items.len()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tokio::time::{timeout, Duration};

    use super::{LandingZone, LandingZoneError};

    #[tokio::test]
    async fn test_add_and_observe() {
        let lz = Arc::new(LandingZone::new(10));

        lz.add("test_item".to_string()).await;

        let observed = lz.observe().await;
        assert_eq!(observed, "test_item");
    }

    #[tokio::test]
    async fn test_observe_blocks_until_item_available() {
        let lz = Arc::new(LandingZone::new(10));
        let lz_clone = lz.clone();

        // Start observing in background.
        let observe_task = tokio::spawn(async move { lz_clone.observe().await });

        // Give it time to start waiting.
        tokio::time::sleep(Duration::from_millis(10)).await;

        lz.add("delayed_item".to_string()).await;

        // Should unblock and return the item.
        let result = timeout(Duration::from_millis(100), observe_task).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap().unwrap(), "delayed_item");
    }

    #[tokio::test]
    async fn test_remove_observed() {
        let lz = Arc::new(LandingZone::new(10));

        lz.add("item1".to_string()).await;
        let _observed = lz.observe().await;

        let removed = lz.remove_observed().unwrap();
        assert_eq!(removed, "item1");
    }

    #[tokio::test]
    async fn test_remove_non_observed_fails() {
        let lz = Arc::new(LandingZone::<String>::new(10));

        let result = lz.remove_observed();
        assert!(matches!(
            result,
            Err(LandingZoneError::RemovingNonObservedElement)
        ));
    }

    #[tokio::test]
    async fn test_remove_all() {
        let lz = Arc::new(LandingZone::new(10));

        lz.add("item1".to_string()).await;
        lz.add("item2".to_string()).await;

        let _observed = lz.observe().await;

        let all_items = lz.remove_all();
        assert_eq!(all_items.len(), 2);
        assert!(all_items.contains(&"item1".to_string()));
        assert!(all_items.contains(&"item2".to_string()));

        assert!(lz.len() == 0);
    }

    #[tokio::test]
    async fn test_semaphore_limits_capacity() {
        let lz = Arc::new(LandingZone::new(2));

        lz.add("item1".to_string()).await;
        lz.add("item2".to_string()).await;

        // Third add should block (test with timeout).
        let mut add_task = tokio::spawn({
            let lz = lz.clone();
            async move {
                lz.add("item3".to_string()).await;
            }
        });
        // Should timeout because semaphore is full.
        tokio::select! {
            _ = &mut add_task => {
                panic!("add_task should not complete while semaphore is full");
            }
            _ = tokio::time::sleep(Duration::from_millis(50)) => {
                // This is expected, the task is still blocked.
            }
        };

        // Remove one item to free up space.
        let _observed = lz.observe().await;
        let _removed = lz.remove_observed().unwrap();

        // Now the add_task should complete.
        add_task.await.unwrap();

        let all_items = lz.remove_all();
        assert_eq!(all_items.len(), 2);
        assert!(all_items.contains(&"item2".to_string()));
        assert!(all_items.contains(&"item3".to_string()));
    }

    #[tokio::test]
    async fn test_reset_observe_with_concurrent_add() {
        let lz = Arc::new(LandingZone::new(10));

        lz.add("item1".to_string()).await;
        lz.add("item2".to_string()).await;
        lz.add("item3".to_string()).await;

        let observed = lz.observe().await;
        assert_eq!(observed, "item1");

        // In another thread, add 4th item.
        let lz_clone = lz.clone();
        let add_task = tokio::spawn(async move {
            lz_clone.add("item4".to_string()).await;
        });

        add_task.await.unwrap();

        lz.reset_observe();

        assert_eq!(lz.observe().await, "item1");
        assert_eq!(lz.observe().await, "item2");
        assert_eq!(lz.observe().await, "item3");
        assert_eq!(lz.observe().await, "item4");
    }

    #[tokio::test]
    async fn test_semaphore_with_observe_reset() {
        let lz = Arc::new(LandingZone::new(2));

        lz.add("item1".to_string()).await;
        lz.add("item2".to_string()).await;

        // Observe one (should not free semaphore permit yet).
        let _observed = lz.observe().await;

        // Adding should still block because permit not released until remove_observed.
        let add_task = tokio::spawn({
            let lz = lz.clone();
            async move {
                lz.add("item3".to_string()).await;
            }
        });

        let result = timeout(Duration::from_millis(50), add_task).await;
        assert!(result.is_err()); // Should timeout.

        // Reset observe (item goes back to queue, still no permit freed).
        lz.reset_observe();

        // Remove observed should fail (nothing observed now).
        assert!(lz.remove_observed().is_err());

        // Only after actually removing an observed item should permit be freed and add_task should complete.
        let _observed_again = lz.observe().await;
        let _removed = lz.remove_observed().unwrap();

        // Remove item_3.
        let _observed_again_2 = lz.observe().await;
        let _removed_2 = lz.remove_observed().unwrap();

        // Now add should work.
        lz.add("item4".to_string()).await;
    }
}
