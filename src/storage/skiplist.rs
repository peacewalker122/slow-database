use crate::storage::log::RecordType;
use std::cell::RefCell;
use std::rc::Rc;

const MAX_LEVEL: usize = 16;
const P: f64 = 0.5;

type NodePtr = Option<Rc<RefCell<Node>>>;

#[derive(Debug)]
struct Node {
    key: Vec<u8>,
    value: (RecordType, Vec<u8>),
    forward: Vec<NodePtr>,
}

impl Node {
    fn new(key: Vec<u8>, value: (RecordType, Vec<u8>), level: usize) -> Self {
        Node {
            key,
            value,
            forward: vec![None; level + 1],
        }
    }

    fn new_head(level: usize) -> Self {
        Node {
            key: Vec::new(),
            value: (RecordType::Put, Vec::new()),
            forward: vec![None; level + 1],
        }
    }
}

#[derive(Debug, Clone)]
pub struct SkipList {
    head: Rc<RefCell<Node>>,
    level: usize,
    len: usize,
}

impl SkipList {
    pub fn new() -> Self {
        SkipList {
            head: Rc::new(RefCell::new(Node::new_head(MAX_LEVEL))),
            level: 0,
            len: 0,
        }
    }

    fn random_level(&self) -> usize {
        let mut level = 0;
        while level < MAX_LEVEL && rand::random::<f64>() < P {
            level += 1;
        }
        level
    }

    pub fn insert(&mut self, key: Vec<u8>, value: (RecordType, Vec<u8>)) {
        let mut update: Vec<Rc<RefCell<Node>>> = vec![Rc::clone(&self.head); MAX_LEVEL + 1];
        let mut current = Rc::clone(&self.head);

        // Find position to insert
        for i in (0..=self.level).rev() {
            loop {
                let next = {
                    let current_node = current.borrow();
                    current_node.forward[i].clone()
                };

                match next {
                    Some(next_node) => {
                        let next_key = next_node.borrow().key.clone();
                        if next_key < key {
                            current = next_node;
                        } else {
                            break;
                        }
                    }
                    None => break,
                }
            }
            update[i] = Rc::clone(&current);
        }

        // Check if key exists and update
        let next_at_level_0 = current.borrow().forward[0].clone();
        if let Some(ref next_node) = next_at_level_0 {
            if next_node.borrow().key == key {
                // Update existing node
                next_node.borrow_mut().value = value;
                return;
            }
        }

        let new_level = self.random_level();
        if new_level > self.level {
            for i in self.level + 1..=new_level {
                update[i] = Rc::clone(&self.head);
            }
            self.level = new_level;
        }

        let new_node = Rc::new(RefCell::new(Node::new(key, value, new_level)));

        // Update forward pointers
        for i in 0..=new_level {
            let update_node = &update[i];
            let old_next = update_node.borrow().forward[i].clone();
            new_node.borrow_mut().forward[i] = old_next;
            update_node.borrow_mut().forward[i] = Some(Rc::clone(&new_node));
        }

        self.len += 1;
    }

    pub fn get(&self, key: &[u8]) -> Option<(RecordType, Vec<u8>)> {
        let mut current = Rc::clone(&self.head);

        for i in (0..=self.level).rev() {
            loop {
                let next = {
                    let current_node = current.borrow();
                    current_node.forward[i].clone()
                };

                match next {
                    Some(next_node) => {
                        let (next_key, next_value) = {
                            let borrowed = next_node.borrow();
                            (borrowed.key.clone(), borrowed.value.clone())
                        };

                        if next_key.as_slice() < key {
                            current = next_node;
                        } else if next_key.as_slice() == key {
                            return Some((next_value.0, next_value.1));
                        } else {
                            break;
                        }
                    }
                    None => break,
                }
            }
        }

        None
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn clear(&mut self) {
        self.head = Rc::new(RefCell::new(Node::new_head(MAX_LEVEL)));
        self.level = 0;
        self.len = 0;
    }

    pub fn iter(&self) -> SkipListIter {
        let first = self.head.borrow().forward[0].clone();
        SkipListIter { current: first }
    }
}

pub struct SkipListIter {
    current: Option<Rc<RefCell<Node>>>,
}

impl Iterator for SkipListIter {
    type Item = (Vec<u8>, (RecordType, Vec<u8>));

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(node) = self.current.take() {
            let (key, value, next) = {
                let borrowed = node.borrow();
                (
                    borrowed.key.clone(),
                    (borrowed.value.0, borrowed.value.1.clone()),
                    borrowed.forward[0].clone(),
                )
            };
            self.current = next;
            Some((key, value))
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_skiplist_insert_and_get() {
        let mut list = SkipList::new();

        list.insert(b"key1".to_vec(), (RecordType::Put, b"value1".to_vec()));
        list.insert(b"key2".to_vec(), (RecordType::Put, b"value2".to_vec()));
        list.insert(b"key3".to_vec(), (RecordType::Put, b"value3".to_vec()));

        assert_eq!(list.len(), 3);
        assert_eq!(
            list.get(b"key1"),
            Some((RecordType::Put, b"value1".to_vec()))
        );
        assert_eq!(
            list.get(b"key2"),
            Some((RecordType::Put, b"value2".to_vec()))
        );
        assert_eq!(
            list.get(b"key3"),
            Some((RecordType::Put, b"value3".to_vec()))
        );
        assert_eq!(list.get(b"key4"), None);
    }

    #[test]
    fn test_skiplist_update() {
        let mut list = SkipList::new();

        list.insert(b"key1".to_vec(), (RecordType::Put, b"value1".to_vec()));
        assert_eq!(
            list.get(b"key1"),
            Some((RecordType::Put, b"value1".to_vec()))
        );

        // Update the same key
        list.insert(b"key1".to_vec(), (RecordType::Put, b"value2".to_vec()));
        assert_eq!(
            list.get(b"key1"),
            Some((RecordType::Put, b"value2".to_vec()))
        );
        assert_eq!(list.len(), 1); // Length should stay 1
    }

    #[test]
    fn test_skiplist_iter() {
        let mut list = SkipList::new();

        list.insert(b"key3".to_vec(), (RecordType::Put, b"value3".to_vec()));
        list.insert(b"key1".to_vec(), (RecordType::Put, b"value1".to_vec()));
        list.insert(b"key2".to_vec(), (RecordType::Put, b"value2".to_vec()));

        let items: Vec<_> = list.iter().collect();
        assert_eq!(items.len(), 3);

        // Should be sorted by key
        assert_eq!(items[0].0, b"key1");
        assert_eq!(items[1].0, b"key2");
        assert_eq!(items[2].0, b"key3");
    }

    #[test]
    fn test_skiplist_clear() {
        let mut list = SkipList::new();

        list.insert(b"key1".to_vec(), (RecordType::Put, b"value1".to_vec()));
        list.insert(b"key2".to_vec(), (RecordType::Put, b"value2".to_vec()));

        assert_eq!(list.len(), 2);

        list.clear();

        assert_eq!(list.len(), 0);
        assert_eq!(list.get(b"key1"), None);
        assert_eq!(list.get(b"key2"), None);
    }
}
