use std::{
    borrow::Borrow,
    cmp::Ordering,
    collections::{BinaryHeap, HashMap},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

#[derive(Debug, Clone)]
pub struct ExpiringHashMap<K, V> {
    hash_map: HashMap<K, HashMapEntry<V>>,
    heap: BinaryHeap<HeapValue<K>>,
    duration: Duration,
}
impl<K, V> ExpiringHashMap<K, V> {
    pub fn new(duration: Duration) -> Self {
        Self {
            hash_map: HashMap::new(),
            heap: BinaryHeap::new(),
            duration,
        }
    }
}
impl<K: Eq + core::hash::Hash + Clone, V> ExpiringHashMap<K, V> {
    pub fn insert(&mut self, key: K, value: V, shared_instant: SharedClone<Instant>) -> Option<V> {
        let now = Instant::now();
        let entry = HashMapEntry {
            local_instant: now,
            shared_instant,
            value,
        };
        match self.hash_map.insert(key.clone(), entry) {
            Some(prev) => Some(prev.value),
            None => {
                self.heap.push(HeapValue { instant: now, key });
                None
            }
        }
    }

    pub fn cleanup(&mut self) {
        let Some(deadline) = Instant::now().checked_sub(self.duration) else {
            return;
        };
        while let Some(HeapValue { instant, .. }) = self.heap.peek() {
            if *instant > deadline {
                return;
            }

            let key = self.heap.pop().expect("We know it is not empty.").key;

            let real_instant = self.hash_map[&key].latest_instant();

            if real_instant > deadline {
                self.heap.push(HeapValue {
                    instant: real_instant,
                    key,
                });
            } else {
                self.hash_map.remove(&key);
            }
        }
    }

    pub fn get<Q>(&mut self, key: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: ?Sized + Eq + std::hash::Hash,
    {
        self.cleanup();
        match self.hash_map.get_mut(key) {
            Some(entry) => {
                entry.local_instant = Instant::now();
                Some(&entry.value)
            }
            None => None,
        }
    }
}

#[derive(Debug, Clone)]
struct HashMapEntry<V> {
    pub local_instant: Instant,
    pub shared_instant: SharedClone<Instant>,
    pub value: V,
}
impl<V> HashMapEntry<V> {
    pub fn latest_instant(&self) -> Instant {
        self.shared_instant.get().max(self.local_instant)
    }
}

#[derive(Debug, Clone)]
struct HeapValue<K> {
    pub instant: Instant,
    pub key: K,
}
impl<K> PartialOrd for HeapValue<K> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl<K> Ord for HeapValue<K> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.instant.cmp(&other.instant).reverse()
    }
}
impl<K> PartialEq for HeapValue<K> {
    fn eq(&self, other: &Self) -> bool {
        self.instant.eq(&other.instant)
    }
}
impl<K> Eq for HeapValue<K> {}

#[derive(Debug, Clone)]
pub struct SharedClone<T> {
    clone: Arc<Mutex<T>>,
}
impl<T: Clone> SharedClone<T> {
    pub fn new(value: T) -> Self {
        Self {
            clone: Arc::new(Mutex::new(value)),
        }
    }

    pub fn set(&self, value: T) {
        *self.clone.lock().unwrap() = value;
    }

    pub fn get(&self) -> T {
        self.clone.lock().unwrap().clone()
    }
}
