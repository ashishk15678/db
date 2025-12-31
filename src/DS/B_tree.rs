use std::cmp::Ordering;

/// B-Tree with minimum degree `t`
pub struct BTree<K: Ord> {
    t: usize,
    root: Box<Node<K>>,
}

#[derive(Debug)]
struct Node<K: Ord> {
    keys: Vec<K>,
    children: Vec<Box<Node<K>>>,
    leaf: bool,
}

impl<K: Ord> Node<K> {
    fn new(leaf: bool) -> Self {
        Self {
            keys: Vec::new(),
            children: Vec::new(),
            leaf,
        }
    }

    fn is_full(&self, t: usize) -> bool {
        self.keys.len() == 2 * t - 1
    }
}

impl<K: Ord> Iterator for Node<K> {
    type Item = K;

    fn next(&mut self) -> Option<Self::Item> {
        self.keys.pop()
    }
}

impl<K: Ord> FromIterator<K> for Node<K> {
    fn from_iter<T: IntoIterator<Item = K>>(iter: T) -> Self {
        let mut node = Self::new(true);
        node.keys.extend(iter);
        node
    }
}

impl<K: Ord> BTree<K> {
    pub fn new(t: usize) -> Self {
        assert!(t >= 2, "B-Tree minimum degree must be >= 2");
        Self {
            t,
            root: Box::new(Node::new(true)),
        }
    }
    pub fn contains(&self, key: &K) -> isize {
        Self::search(&self.root, key)
    }

    pub fn insert(&mut self, key: K) {
        if self.root.is_full(self.t) {
            let mut new_root = Box::new(Node::new(false));
            let old_root = std::mem::replace(&mut self.root, Box::new(Node::new(true)));
            new_root.children.push(old_root);

            Self::split_child(&mut new_root, 0, self.t);
            self.root = new_root;
        }

        Self::insert_non_full(&mut self.root, key, self.t);
    }

    fn search(node: &Node<K>, key: &K) -> isize {
        match node.keys.binary_search(key) {
            Ok(idx) => idx as isize,
            Err(idx) => {
                if node.leaf {
                    -1
                } else {
                    Self::search(&node.children[idx], key)
                }
            }
        }
    }

    fn insert_non_full(node: &mut Box<Node<K>>, key: K, t: usize) {
        if node.leaf {
            let pos = node.keys.binary_search(&key).unwrap_or_else(|i| i);
            node.keys.insert(pos, key);
            return;
        }

        let mut idx = node.keys.binary_search(&key).unwrap_or_else(|i| i);

        if node.children[idx].is_full(t) {
            Self::split_child(node, idx, t);

            if key > node.keys[idx] {
                idx += 1;
            }
        }

        Self::insert_non_full(&mut node.children[idx], key, t);
    }

    fn split_child(parent: &mut Box<Node<K>>, idx: usize, t: usize) {
        let mut full_child = parent.children.remove(idx);

        let mut new_child = Box::new(Node::new(full_child.leaf));

        // Median key moves up
        let median = full_child.keys.remove(t - 1);

        // Move keys
        new_child.keys = full_child.keys.split_off(t - 1);

        // Move children if internal node
        if !full_child.leaf {
            new_child.children = full_child.children.split_off(t);
        }

        parent.keys.insert(idx, median);
        parent.children.insert(idx, full_child);
        parent.children.insert(idx + 1, new_child);
    }
}
