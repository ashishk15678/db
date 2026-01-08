// B+ Tree implementation for disk-based storage
// Leaf nodes contain actual data, internal nodes contain only keys for navigation

use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use lru::LruCache;

/// Page size in bytes (4KB)
pub const PAGE_SIZE: usize = 4096;

/// B+ tree order (max keys per node)
pub const BTREE_ORDER: usize = 32;

/// Magic bytes for file identification
pub const MAGIC: &[u8; 4] = b"BFLY";

/// File header size
pub const HEADER_SIZE: usize = PAGE_SIZE;

/// Maximum pages to cache in memory (4MB with 4KB pages)
pub const MAX_CACHE_PAGES: usize = 1024;

/// Page types
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[repr(u8)]
pub enum PageType {
    Free = 0,
    Internal = 1,
    Leaf = 2,
    Overflow = 3,
}

/// File header stored at the beginning of the data file
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileHeader {
    pub magic: [u8; 4],
    pub version: u32,
    pub page_size: u32,
    pub total_pages: u64,
    pub free_page_list: u64,
    pub root_page: u64,
}

impl Default for FileHeader {
    fn default() -> Self {
        Self {
            magic: *MAGIC,
            version: 1,
            page_size: PAGE_SIZE as u32,
            total_pages: 1, // Header page
            free_page_list: 0,
            root_page: 0,
        }
    }
}

/// A key-value pair stored in leaf nodes
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeyValue {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

/// Internal node entry (key + child page pointer)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InternalEntry {
    pub key: Vec<u8>,
    pub child_page: u64,
}

/// B+ tree node stored in a page
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BPlusNode {
    Internal {
        entries: Vec<InternalEntry>,
        /// Rightmost child pointer
        right_child: u64,
    },
    Leaf {
        entries: Vec<KeyValue>,
        /// Next leaf page for range scans
        next_leaf: u64,
        /// Previous leaf page
        prev_leaf: u64,
    },
}

impl BPlusNode {
    pub fn new_leaf() -> Self {
        BPlusNode::Leaf {
            entries: Vec::new(),
            next_leaf: 0,
            prev_leaf: 0,
        }
    }

    pub fn new_internal() -> Self {
        BPlusNode::Internal {
            entries: Vec::new(),
            right_child: 0,
        }
    }

    /// Serialize node to bytes
    pub fn serialize(&self) -> Vec<u8> {
        bincode::serialize(self).unwrap_or_default()
    }

    /// Deserialize node from bytes
    pub fn deserialize(bytes: &[u8]) -> Option<Self> {
        bincode::deserialize(bytes).ok()
    }

    pub fn is_leaf(&self) -> bool {
        matches!(self, BPlusNode::Leaf { .. })
    }

    pub fn len(&self) -> usize {
        match self {
            BPlusNode::Internal { entries, .. } => entries.len(),
            BPlusNode::Leaf { entries, .. } => entries.len(),
        }
    }

    pub fn is_full(&self) -> bool {
        self.len() >= BTREE_ORDER - 1
    }
}

/// Page on disk
#[derive(Debug)]
pub struct DiskPage {
    pub page_id: u64,
    pub page_type: PageType,
    pub data: Vec<u8>,
}

impl DiskPage {
    pub fn new(page_id: u64, page_type: PageType) -> Self {
        Self {
            page_id,
            page_type,
            data: vec![0u8; PAGE_SIZE],
        }
    }

    pub fn from_node(page_id: u64, node: &BPlusNode) -> Self {
        let mut page = Self::new(
            page_id,
            if node.is_leaf() {
                PageType::Leaf
            } else {
                PageType::Internal
            },
        );
        let serialized = node.serialize();
        let len = serialized.len().min(PAGE_SIZE - 8);
        page.data[0] = page.page_type as u8;
        page.data[1..5].copy_from_slice(&(len as u32).to_le_bytes());
        page.data[8..8 + len].copy_from_slice(&serialized[..len]);
        page
    }

    pub fn to_node(&self) -> Option<BPlusNode> {
        if self.data.len() < 8 {
            return None;
        }
        let len = u32::from_le_bytes([self.data[1], self.data[2], self.data[3], self.data[4]]) as usize;
        if len == 0 || 8 + len > self.data.len() {
            return None;
        }
        BPlusNode::deserialize(&self.data[8..8 + len])
    }
}

/// Pager manages reading and writing pages to disk
pub struct Pager {
    file: File,
    header: FileHeader,
    cache: LruCache<u64, DiskPage>,
    path: PathBuf,
    dirty_pages: Vec<u64>,  // Track pages that need flushing
}

impl Pager {
    /// Open or create a data file
    pub fn open(path: PathBuf) -> std::io::Result<Self> {
        let file_exists = path.exists();

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)?;

        let header = if file_exists && file.metadata()?.len() >= HEADER_SIZE as u64 {
            // Read existing header
            let mut header_bytes = vec![0u8; HEADER_SIZE];
            file.seek(SeekFrom::Start(0))?;
            file.read_exact(&mut header_bytes)?;
            
            bincode::deserialize(&header_bytes).unwrap_or_default()
        } else {
            // Create new header
            let header = FileHeader::default();
            let header_bytes = bincode::serialize(&header).unwrap_or_default();
            let mut page = vec![0u8; HEADER_SIZE];
            page[..header_bytes.len()].copy_from_slice(&header_bytes);
            
            file.seek(SeekFrom::Start(0))?;
            file.write_all(&page)?;
            file.sync_all()?;
            
            header
        };

        Ok(Self {
            file,
            header,
            cache: LruCache::new(NonZeroUsize::new(MAX_CACHE_PAGES).unwrap()),
            path,
            dirty_pages: Vec::new(),
        })
    }

    /// Allocate a new page
    pub fn allocate_page(&mut self) -> std::io::Result<u64> {
        let page_id = self.header.total_pages;
        self.header.total_pages += 1;
        self.write_header()?;
        
        // Initialize empty page
        let page = DiskPage::new(page_id, PageType::Free);
        self.write_page(&page)?;
        
        Ok(page_id)
    }

    /// Read a page from disk or cache
    pub fn read_page(&mut self, page_id: u64) -> std::io::Result<&DiskPage> {
        if !self.cache.contains(&page_id) {
            let offset = HEADER_SIZE as u64 + (page_id - 1) * PAGE_SIZE as u64;
            let mut data = vec![0u8; PAGE_SIZE];
            
            self.file.seek(SeekFrom::Start(offset))?;
            self.file.read_exact(&mut data)?;
            
            let page_type = match data[0] {
                1 => PageType::Internal,
                2 => PageType::Leaf,
                3 => PageType::Overflow,
                _ => PageType::Free,
            };
            
            let page = DiskPage {
                page_id,
                page_type,
                data,
            };
            
            self.cache.put(page_id, page);
        }
        
        Ok(self.cache.get(&page_id).unwrap())
    }

    /// Write a page to disk
    pub fn write_page(&mut self, page: &DiskPage) -> std::io::Result<()> {
        let offset = HEADER_SIZE as u64 + (page.page_id - 1) * PAGE_SIZE as u64;
        
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.write_all(&page.data)?;
        
        // Update cache
        self.cache.put(page.page_id, DiskPage {
            page_id: page.page_id,
            page_type: page.page_type,
            data: page.data.clone(),
        });
        
        Ok(())
    }

    /// Write header to disk
    fn write_header(&mut self) -> std::io::Result<()> {
        let header_bytes = bincode::serialize(&self.header).unwrap_or_default();
        let mut page = vec![0u8; HEADER_SIZE];
        page[..header_bytes.len()].copy_from_slice(&header_bytes);
        
        self.file.seek(SeekFrom::Start(0))?;
        self.file.write_all(&page)?;
        
        Ok(())
    }

    /// Sync all changes to disk
    pub fn sync(&mut self) -> std::io::Result<()> {
        self.write_header()?;
        self.file.sync_all()
    }

    /// Get the root page ID
    pub fn root_page(&self) -> u64 {
        self.header.root_page
    }

    /// Set the root page ID
    pub fn set_root_page(&mut self, page_id: u64) -> std::io::Result<()> {
        self.header.root_page = page_id;
        self.write_header()
    }

    /// Invalidate cache for a page
    pub fn invalidate(&mut self, page_id: u64) {
        self.cache.pop(&page_id);
    }
}

/// B+ tree backed by disk pages
pub struct BPlusTree {
    pager: Pager,
    table_name: String,
}

impl BPlusTree {
    /// Open or create a B+ tree for a table
    pub fn open(data_dir: PathBuf, table_name: &str) -> std::io::Result<Self> {
        std::fs::create_dir_all(&data_dir)?;
        let path = data_dir.join(format!("{}.db", table_name));
        let mut pager = Pager::open(path)?;
        
        // Create root if needed
        if pager.root_page() == 0 {
            let root_id = pager.allocate_page()?;
            let root = BPlusNode::new_leaf();
            let page = DiskPage::from_node(root_id, &root);
            pager.write_page(&page)?;
            pager.set_root_page(root_id)?;
        }
        
        Ok(Self {
            pager,
            table_name: table_name.to_string(),
        })
    }

    /// Insert a key-value pair
    pub fn insert(&mut self, key: Vec<u8>, value: Vec<u8>) -> std::io::Result<()> {
        let root_id = self.pager.root_page();
        
        // Read root node
        let root_page = self.pager.read_page(root_id)?;
        let mut root = root_page.to_node().unwrap_or_else(BPlusNode::new_leaf);
        
        if root.is_full() {
            // Need to split root
            let new_root_id = self.pager.allocate_page()?;
            let new_child_id = self.pager.allocate_page()?;
            
            // Move old root to new child, create new root
            let (median_key, right_node) = self.split_node(&mut root)?;
            
            // Write old root (now left child)
            let left_page = DiskPage::from_node(root_id, &root);
            self.pager.write_page(&left_page)?;
            
            // Write right child
            let right_page = DiskPage::from_node(new_child_id, &right_node);
            self.pager.write_page(&right_page)?;
            
            // Create new root
            let new_root = BPlusNode::Internal {
                entries: vec![InternalEntry {
                    key: median_key.clone(),
                    child_page: root_id,
                }],
                right_child: new_child_id,
            };
            
            let new_root_page = DiskPage::from_node(new_root_id, &new_root);
            self.pager.write_page(&new_root_page)?;
            self.pager.set_root_page(new_root_id)?;
            
            // Now insert into appropriate child
            if key <= median_key {
                self.insert_into_node(root_id, key, value)?;
            } else {
                self.insert_into_node(new_child_id, key, value)?;
            }
        } else {
            self.insert_into_node(root_id, key, value)?;
        }
        
        // Note: sync() is NOT called here for performance
        // Call sync() explicitly or use batch_insert() for durability
        Ok(())
    }

    /// Insert multiple key-value pairs with a single sync at the end
    /// This is much faster than individual inserts for bulk operations
    pub fn batch_insert(&mut self, entries: Vec<(Vec<u8>, Vec<u8>)>) -> std::io::Result<usize> {
        let count = entries.len();
        for (key, value) in entries {
            // Use internal insert without sync
            let root_id = self.pager.root_page();
            let root_page = self.pager.read_page(root_id)?;
            let root = root_page.to_node().unwrap_or_else(BPlusNode::new_leaf);
            
            if root.is_full() {
                // Handle split case - simplified, just call regular insert
                self.insert(key, value)?;
            } else {
                self.insert_into_node(root_id, key, value)?;
            }
        }
        // Single sync at the end
        self.pager.sync()?;
        Ok(count)
    }

    fn insert_into_node(&mut self, page_id: u64, key: Vec<u8>, value: Vec<u8>) -> std::io::Result<()> {
        self.pager.invalidate(page_id);
        let page = self.pager.read_page(page_id)?;
        let mut node = page.to_node().unwrap_or_else(BPlusNode::new_leaf);
        
        match &mut node {
            BPlusNode::Leaf { entries, .. } => {
                // Find insertion point
                let pos = entries.iter().position(|e| e.key > key).unwrap_or(entries.len());
                
                // Check if key exists and update
                if pos > 0 && entries[pos - 1].key == key {
                    entries[pos - 1].value = value;
                } else {
                    entries.insert(pos, KeyValue { key, value });
                }
                
                let page = DiskPage::from_node(page_id, &node);
                self.pager.write_page(&page)?;
            }
            BPlusNode::Internal { entries, right_child } => {
                // Find child to descend into
                let child_id = if let Some(entry) = entries.iter().find(|e| key <= e.key) {
                    entry.child_page
                } else {
                    *right_child
                };
                
                // Recurse
                self.insert_into_node(child_id, key, value)?;
            }
        }
        
        Ok(())
    }

    fn split_node(&mut self, node: &mut BPlusNode) -> std::io::Result<(Vec<u8>, BPlusNode)> {
        match node {
            BPlusNode::Leaf { entries, next_leaf, prev_leaf: _ } => {
                let mid = entries.len() / 2;
                let right_entries = entries.split_off(mid);
                let median_key = right_entries[0].key.clone();
                
                let right_node = BPlusNode::Leaf {
                    entries: right_entries,
                    next_leaf: *next_leaf,
                    prev_leaf: 0, // Will be set when writing
                };
                
                Ok((median_key, right_node))
            }
            BPlusNode::Internal { entries, right_child } => {
                let mid = entries.len() / 2;
                let median = entries.remove(mid);
                let right_entries = entries.split_off(mid);
                
                let right_node = BPlusNode::Internal {
                    entries: right_entries,
                    right_child: *right_child,
                };
                
                *right_child = median.child_page;
                
                Ok((median.key, right_node))
            }
        }
    }

    /// Search for a key
    pub fn get(&mut self, key: &[u8]) -> std::io::Result<Option<Vec<u8>>> {
        let root_id = self.pager.root_page();
        self.search_node(root_id, key)
    }

    fn search_node(&mut self, page_id: u64, key: &[u8]) -> std::io::Result<Option<Vec<u8>>> {
        let page = self.pager.read_page(page_id)?;
        let node = match page.to_node() {
            Some(n) => n,
            None => return Ok(None),
        };
        
        match node {
            BPlusNode::Leaf { entries, .. } => {
                for entry in entries {
                    if entry.key == key {
                        return Ok(Some(entry.value));
                    }
                }
                Ok(None)
            }
            BPlusNode::Internal { entries, right_child } => {
                for entry in &entries {
                    if key <= &entry.key[..] {
                        return self.search_node(entry.child_page, key);
                    }
                }
                self.search_node(right_child, key)
            }
        }
    }

    /// Delete a key
    pub fn delete(&mut self, key: &[u8]) -> std::io::Result<bool> {
        let root_id = self.pager.root_page();
        let deleted = self.delete_from_node(root_id, key)?;
        self.pager.sync()?;
        Ok(deleted)
    }

    fn delete_from_node(&mut self, page_id: u64, key: &[u8]) -> std::io::Result<bool> {
        self.pager.invalidate(page_id);
        let page = self.pager.read_page(page_id)?;
        let mut node = match page.to_node() {
            Some(n) => n,
            None => return Ok(false),
        };
        
        match &mut node {
            BPlusNode::Leaf { entries, .. } => {
                let initial_len = entries.len();
                entries.retain(|e| e.key != key);
                let deleted = entries.len() < initial_len;
                
                if deleted {
                    let page = DiskPage::from_node(page_id, &node);
                    self.pager.write_page(&page)?;
                }
                
                Ok(deleted)
            }
            BPlusNode::Internal { entries, right_child } => {
                for entry in entries.iter() {
                    if key <= &entry.key[..] {
                        return self.delete_from_node(entry.child_page, key);
                    }
                }
                self.delete_from_node(*right_child, key)
            }
        }
    }

    /// Scan all key-value pairs
    pub fn scan<F>(&mut self, mut callback: F) -> std::io::Result<()>
    where
        F: FnMut(&[u8], &[u8]),
    {
        let root_id = self.pager.root_page();
        self.scan_node(root_id, &mut callback)
    }

    fn scan_node<F>(&mut self, page_id: u64, callback: &mut F) -> std::io::Result<()>
    where
        F: FnMut(&[u8], &[u8]),
    {
        let page = self.pager.read_page(page_id)?;
        let node = match page.to_node() {
            Some(n) => n,
            None => return Ok(()),
        };
        
        match node {
            BPlusNode::Leaf { entries, .. } => {
                for entry in entries {
                    callback(&entry.key, &entry.value);
                }
            }
            BPlusNode::Internal { entries, right_child } => {
                for entry in &entries {
                    self.scan_node(entry.child_page, callback)?;
                }
                self.scan_node(right_child, callback)?;
            }
        }
        
        Ok(())
    }

    /// Count total entries
    pub fn count(&mut self) -> std::io::Result<usize> {
        let mut count = 0;
        self.scan(|_, _| count += 1)?;
        Ok(count)
    }

    /// Sync to disk
    pub fn sync(&mut self) -> std::io::Result<()> {
        self.pager.sync()
    }
}

/// Thread-safe B+ tree wrapper
#[derive(Clone)]
pub struct SharedBPlusTree {
    pub inner: Arc<RwLock<BPlusTree>>,
}

impl SharedBPlusTree {
    pub fn open(data_dir: PathBuf, table_name: &str) -> std::io::Result<Self> {
        let tree = BPlusTree::open(data_dir, table_name)?;
        Ok(Self {
            inner: Arc::new(RwLock::new(tree)),
        })
    }

    pub fn insert(&self, key: Vec<u8>, value: Vec<u8>) -> Result<(), String> {
        self.inner
            .write()
            .map_err(|e| e.to_string())?
            .insert(key, value)
            .map_err(|e| e.to_string())
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, String> {
        self.inner
            .write()
            .map_err(|e| e.to_string())?
            .get(key)
            .map_err(|e| e.to_string())
    }

    pub fn delete(&self, key: &[u8]) -> Result<bool, String> {
        self.inner
            .write()
            .map_err(|e| e.to_string())?
            .delete(key)
            .map_err(|e| e.to_string())
    }

    pub fn scan<F>(&self, callback: F) -> Result<(), String>
    where
        F: FnMut(&[u8], &[u8]),
    {
        self.inner
            .write()
            .map_err(|e| e.to_string())?
            .scan(callback)
            .map_err(|e| e.to_string())
    }

    pub fn count(&self) -> Result<usize, String> {
        self.inner
            .write()
            .map_err(|e| e.to_string())?
            .count()
            .map_err(|e| e.to_string())
    }

    /// Batch insert multiple key-value pairs with single sync
    pub fn batch_insert(&self, entries: Vec<(Vec<u8>, Vec<u8>)>) -> Result<usize, String> {
        self.inner
            .write()
            .map_err(|e| e.to_string())?
            .batch_insert(entries)
            .map_err(|e| e.to_string())
    }

    /// Explicitly sync changes to disk
    pub fn sync(&self) -> Result<(), String> {
        self.inner
            .write()
            .map_err(|e| e.to_string())?
            .sync()
            .map_err(|e| e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    static TEST_COUNTER: AtomicUsize = AtomicUsize::new(0);

    fn test_dir() -> PathBuf {
        let count = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
        std::env::temp_dir().join(format!("btree_test_{}", count))
    }

    #[test]
    fn test_bplus_node_serialize() {
        let node = BPlusNode::new_leaf();
        let bytes = node.serialize();
        let restored = BPlusNode::deserialize(&bytes).unwrap();
        assert!(restored.is_leaf());
    }

    #[test]
    fn test_bplus_tree_insert_get() {
        let dir = test_dir();
        let mut tree = BPlusTree::open(dir.clone(), "test").unwrap();
        
        tree.insert(b"key1".to_vec(), b"value1".to_vec()).unwrap();
        tree.insert(b"key2".to_vec(), b"value2".to_vec()).unwrap();
        
        let val = tree.get(b"key1").unwrap();
        assert_eq!(val, Some(b"value1".to_vec()));
        
        let val = tree.get(b"key2").unwrap();
        assert_eq!(val, Some(b"value2".to_vec()));
        
        let val = tree.get(b"key3").unwrap();
        assert!(val.is_none());
        
        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn test_bplus_tree_delete() {
        let dir = test_dir();
        let mut tree = BPlusTree::open(dir.clone(), "test").unwrap();
        
        tree.insert(b"key1".to_vec(), b"value1".to_vec()).unwrap();
        assert!(tree.delete(b"key1").unwrap());
        assert!(tree.get(b"key1").unwrap().is_none());
        
        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn test_bplus_tree_scan() {
        let dir = test_dir();
        let mut tree = BPlusTree::open(dir.clone(), "test").unwrap();
        
        tree.insert(b"a".to_vec(), b"1".to_vec()).unwrap();
        tree.insert(b"b".to_vec(), b"2".to_vec()).unwrap();
        tree.insert(b"c".to_vec(), b"3".to_vec()).unwrap();
        
        let mut count = 0;
        tree.scan(|_, _| count += 1).unwrap();
        assert_eq!(count, 3);
        
        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn test_bplus_tree_persistence() {
        let dir = test_dir();
        
        // Write data
        {
            let mut tree = BPlusTree::open(dir.clone(), "persist").unwrap();
            tree.insert(b"persist_key".to_vec(), b"persist_value".to_vec()).unwrap();
            tree.sync().unwrap();
        }
        
        // Read back
        {
            let mut tree = BPlusTree::open(dir.clone(), "persist").unwrap();
            let val = tree.get(b"persist_key").unwrap();
            assert_eq!(val, Some(b"persist_value".to_vec()));
        }
        
        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn test_bplus_tree_many_inserts() {
        let dir = test_dir();
        let mut tree = BPlusTree::open(dir.clone(), "many").unwrap();
        
        for i in 0..100 {
            let key = format!("key_{:04}", i).into_bytes();
            let value = format!("value_{}", i).into_bytes();
            tree.insert(key, value).unwrap();
        }
        
        assert_eq!(tree.count().unwrap(), 100);
        
        // Verify some values
        let val = tree.get(b"key_0050").unwrap();
        assert_eq!(val, Some(b"value_50".to_vec()));
        
        std::fs::remove_dir_all(dir).ok();
    }
}
