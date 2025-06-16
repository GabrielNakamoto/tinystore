use anyhow::{anyhow, Result};
use bincode::{config::BigEndian, Decode, Encode};
use log::{debug, info};
use std::fs::File;
use std::io::{Seek, SeekFrom};
use std::os::unix::fs::FileExt;
use std::path::Path;

type PageId = u32;

/// Index or 'id' of an item and its corresponding offset, page local
///
/// Starts at 0 at back of page and increases towards offset array
type ItemPtr = usize;
type Key = Vec<u8>;
type Value = Vec<u8>;

const BINCODE_CONFIG: bincode::config::Configuration<BigEndian> =
    bincode::config::standard().with_big_endian();
const PAGE_SIZE: usize = 4096;
///
/// Page Header:
///
/// (1) # items
/// (2) mgic numbers (different for first page)
///
const PAGE_HEADER_SIZE: usize = 6;
const METADATA_SIZE: usize = 18;
const MAGIC: u32 = 0x54494E59;
const CACHE_CAPACITY: u16 = 10;

#[derive(Encode, Decode, Debug)]
struct MetaData {
    magic: u32,
    size: u64,
    root: PageId,
    height: u16,
}

///
/// Could also be called node, abstraction
/// for page level operations
///
struct PageData {
    buf: [u8; PAGE_SIZE],
}

///  Items Stored as:
///
///   ------------------------------------
///  | key_len | value_len | key | value |
///  -------------------------------------
///
///  Where value is either:
///
///  (1) arbitrary size byte array
///  (2) child page id / ptr
///
///
/// *Use empty key for the n+1th internal node child ptr

/// In internal nodes store the leftmost child pointer
/// without a key that way you never have to handle half items
/// because that node will always split to the right

impl PageData {
    fn new() -> PageData {
        PageData {
            buf: [0u8; PAGE_SIZE],
        }
    }

    pub fn print_items(&self) {
        for i in 0..self.get_n_items() {
            let (key, value) = self.get_item(i);
            info!("\tItem {i}: ({key:?}, {value:?})");
        }
    }

    // pub fn search(&self, target: &Key) -> Option<(Key, Value)> {
    //     for i in 0..self.get_n_items() {
    //         let (key, value) = self.get_item(i);
    //         if key == *target {
    //             return Some((key, value));
    //         }
    //     }
    //     None
    // }

    pub fn lin_find_place(&self, key: &Key) -> ItemPtr {
        let n = self.get_n_items();
        &for i in 0..n {
            if !self.gt_entry(key, i) {
                return i;
            }
        };

        n
    }
    ///
    /// Evaluates a greater than comparison between 2 items,
    /// returning true if the left is greater
    ///
    pub fn gt_entry(&self, lkey: &Key, ip: ItemPtr) -> bool {
        let (rkey, _) = self.get_item(ip);

        *lkey > rkey
    }

    pub fn as_slice(&self) -> &[u8] {
        self.buf.as_slice()
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        self.buf.as_mut_slice()
    }

    pub fn copy_into(&mut self, src: &mut [u8]) {
        self.buf.copy_from_slice(src);
    }

    pub fn append_item(&mut self, key: &Key, value: &Value) -> bool {
        self.insert_item(self.get_n_items(), key, value)
    }

    pub fn insert_item(&mut self, ip: ItemPtr, key: &Key, value: &Value) -> bool {
        let n_items = self.get_n_items();
        let size = self.get_size();
        let kl = key.len();
        let vl = value.len();
        let il = 4 + kl + vl;

        if PAGE_SIZE - PAGE_HEADER_SIZE > (n_items * 2) + size + il + 2 {
            // Shift greater items towards header,
            // offsets will decrease
            for i in (ip..n_items).rev() {
                self.set_offs(i + 1, self.get_offs(i) - il);
            }

            let offs = if ip == 0 {
                PAGE_SIZE - il
            } else {
                self.get_offs(ip - 1) - il
            };

            if ip != n_items {
                let start = self.get_offs(n_items);
                self.buf.copy_within(start..offs + il, start - il);
            }

            self.set_offs(ip, offs);
            self.set_u16(offs, kl as u16);
            self.set_u16(offs + 2, vl as u16);

            let offs = offs + 4;
            self.buf[offs..offs + kl].copy_from_slice(key.as_slice());
            self.buf[offs + kl..offs + kl + vl].copy_from_slice(value.as_slice());

            self.set_n_items(n_items + 1);

            true
        } else {
            false
        }
    }

    ///
    /// Splits entries into two halves based on the ceil split point,
    /// moves entries greater than split point to new page and returns
    /// the pointer to the split entry. Split entry remains in source
    /// page.
    ///
    pub fn split(&mut self) -> (ItemPtr, PageData) {
        let mut right = PageData::new();
        let n_items = self.get_n_items();
        let sp = ((n_items + 1) / 2) - 1;

        for i in (sp..n_items).rev() {
            let (key, value) = self.remove_item(i);
            right.insert_item(0, &key, &value);
        }

        (sp - 1, right)
    }

    pub fn remove_item(&mut self, ip: ItemPtr) -> (Key, Value) {
        let n_items = self.get_n_items();
        let ioffs = self.get_offs(ip);
        let start = self.get_offs(n_items - 1);
        let offs = PAGE_HEADER_SIZE + (2 * ip);
        let (key, value) = self.get_item(ip);
        let il = key.len() + value.len() + 4;

        // Update other items offsets
        for i in ip..n_items {
            // Shift all items to the 'right' by item length
            self.set_offs(i, self.get_offs(i) + il);
        }

        let free_start = PAGE_HEADER_SIZE + ((n_items + 1) * 2);

        // Shift offsets left
        self.buf.copy_within(offs + 2..free_start, offs);

        // Shift data
        self.buf.copy_within(start..ioffs, start + il);

        self.set_n_items(n_items - 1);

        (key, value)
    }

    pub fn get_item(&self, ip: ItemPtr) -> (Key, Value) {
        let offs = self.get_offs(ip);
        let kl = self.get_u16(offs) as usize;
        let vl = self.get_u16(offs + 2) as usize;

        let offs = offs + 4;
        (
            self.buf[offs..offs + kl].to_vec(),
            self.buf[offs + kl..offs + kl + vl].to_vec(),
        )
    }

    ///
    /// Returns total byte size of every item in the page,
    /// not including 2 byte offset entry
    ///
    ///
    fn get_size(&self) -> usize {
        let mut size: usize = 0;
        for i in 0..self.get_n_items() {
            let (key, value) = self.get_item(i);
            size += key.len() + value.len() + 4;
        }

        size
    }

    fn get_child(&self, ip: ItemPtr) -> u32 {
        let (key, value) = self.get_item(ip);
        u32::from_be_bytes(value.try_into().unwrap())
    }

    fn set_n_items(&mut self, data: usize) {
        self.set_u16(0, data as u16)
    }

    pub fn get_n_items(&self) -> usize {
        self.get_u16(0) as usize
    }

    fn set_offs(&mut self, ip: ItemPtr, data: usize) {
        let offs = PAGE_HEADER_SIZE + (ip * 2);
        self.set_u16(offs, data as u16);
    }

    fn get_offs(&self, ip: ItemPtr) -> usize {
        let offs = PAGE_HEADER_SIZE + (ip * 2);

        self.get_u16(offs) as usize
    }

    fn set_u16(&mut self, offs: usize, data: u16) {
        self.buf[offs..offs + 2].copy_from_slice(&data.to_be_bytes());
    }

    fn get_u16(&self, offs: usize) -> u16 {
        u16::from_be_bytes(self.buf[offs..offs + 2].try_into().unwrap())
    }

    fn set_u32(&mut self, offs: usize, data: u32) {
        self.buf[offs..offs + 4].copy_from_slice(&data.to_be_bytes());
    }

    fn get_u32(&self, offs: usize) -> u32 {
        u32::from_be_bytes(self.buf[offs..offs + 4].try_into().unwrap())
    }
}

struct BTree {
    root: PageId,
    pub height: u16,
}

impl BTree {
    pub fn initialize(meta: &MetaData) -> BTree {
        BTree {
            root: meta.root,
            height: meta.height,
        }
    }

    fn create_root(&mut self, io: &mut PageCache, overflow: (Key, PageData)) -> Result<()> {
        let (sk, right) = overflow;
        let pid = self.root;

        let page = io.get_page(pid)?;
        let right_id = io.new_page()?;
        let root_id = io.new_page()?;
        info!("Creating root at page id {root_id}");

        let mut root = PageData::new();
        root.insert_item(0, &sk, &pid.to_be_bytes().to_vec());
        root.insert_item(1, &vec![0u8; 0], &right_id.to_be_bytes().to_vec());

        io.commit_page(right_id, &right)?;
        io.commit_page(root_id, &root)?;

        self.root = root_id;
        self.height += 1;

        Ok(())
    }

    pub fn btree_get(&self, io: &mut PageCache, key: &Key) -> Result<Value> {
        let pid = self.find_leaf(io, self.root, key, self.height - 1)?;
        let page = io.get_page(pid)?;

        // TODO: binary search within pages
        for i in 0..page.get_n_items() {
            let (k, v) = page.get_item(i);
            if k == *key {
                return Ok(v);
            }
        }

        Err(anyhow!("Couldn't find entry with requested key"))
    }

    fn find_leaf(
        &self,
        io: &mut PageCache,
        mut pid: PageId,
        key: &Key,
        height: u16,
    ) -> Result<PageId> {
        if height == 0 {
            Ok(pid)
        } else {
            let page = io.get_page(pid)?;
            let ip = page.lin_find_place(key);
            pid = page.get_child(ip.min(page.get_n_items() - 1));
            self.find_leaf(io, pid, key, height - 1)
        }
    }

    pub fn btree_insert(&mut self, io: &mut PageCache, key: &Key, value: &Value) -> Result<()> {
        if self.root == 0 {
            let mut root = PageData::new();
            root.insert_item(0, key, value);
            io.commit_page(1, &root);
            self.root = 1;
            self.height = 1;
            return Ok(());
        }

        if let Some(root_overflow) = self.insert(io, self.root, key, value, self.height - 1)? {
            self.create_root(io, root_overflow)?;
        }

        Ok(())
    }

    fn balance(
        &mut self,
        io: &mut PageCache,
        ip: ItemPtr,
        sk: Key,
        parent: &mut PageData,
        child: &mut PageData,
        mut right: PageData,
        key: &Key,
        value: &Value,
        pid: PageId,
        cid: PageId,
        height: u16,
    ) -> Result<Option<(Key, PageData)>> {
        let rid = io.new_page()?;
        io.commit_page(rid, &right);

        // Swap keys with child pointers
        //
        //  |....| .... | left child ptr | Key 1 | ....
        //  |....| .... | left child ptr | Key 2 | Right child ptr | Key 1 | ....
        //
        let (k1, _) = parent.remove_item(ip);

        // Can this overflow?
        parent.insert_item(ip, &sk, &cid.to_be_bytes().to_vec());

        let overflow = self.try_insert(
            io,
            parent,
            pid,
            ip + 1,
            &k1,
            &rid.to_be_bytes().to_vec(),
            height,
        );

        io.commit_page(pid, &parent)?;

        Ok(overflow)
    }

    fn insert(
        &mut self,
        io: &mut PageCache,
        pid: PageId,
        key: &Key,
        value: &Value,
        height: u16,
    ) -> Result<Option<(Key, PageData)>> {
        let mut page = io.get_page(pid)?;
        let n = page.get_n_items();
        let ip = page.lin_find_place(key);

        if height == 0 {
            let overflow = self.try_insert(io, &mut page, pid, ip, key, value, height);
            io.commit_page(pid, &page)?;
            return Ok(overflow);
        }

        let child = page.get_child(ip.min(n - 1)) as PageId;
        let mut child_page = io.get_page(child)?;

        let overflow = if height == 1 {
            let cip = child_page.lin_find_place(key);
            let over = self.try_insert(io, &mut child_page, child, cip, key, value, height - 1);

            // Only save the newest version
            io.commit_page(child, &child_page);

            over
        } else {
            self.insert(io, child, key, value, height - 1)?
        };

        page = io.get_page(pid)?;
        if let Some((sk, right)) = overflow {
            self.balance(
                io,
                ip.min(n - 1),
                sk,
                &mut page,
                &mut child_page,
                right,
                key,
                value,
                pid,
                child,
                height,
            )
        } else {
            Ok(None)
        }
    }

    pub fn try_insert(
        &mut self,
        io: &mut PageCache,
        page: &mut PageData,
        pid: PageId,
        ip: ItemPtr,
        key: &Key,
        value: &Value,
        height: u16,
    ) -> Option<(Key, PageData)> {
        // Return split key and right node page data
        let okay = page.insert_item(ip, key, value);
        io.commit_page(pid, &page);
        if !okay {
            let (mut sp, mut right) = page.split();
            let (sk, sv) = if ip == sp + 1 {
                sp += 1;
                (key.clone(), value.clone())
            } else {
                page.remove_item(sp)
            };

            if height >= 1 {
                page.insert_item(sp, &vec![0u8; 0], &sv);
            } else {
                page.insert_item(sp, &sk, &sv);
            }

            if *key > sk {
                let ip = right.lin_find_place(key);
                right.insert_item(ip, &key, &value);
            } else if *key < sk {
                let ip = page.lin_find_place(key);
                page.insert_item(ip, &key, &value);
            }

            return Some((sk, right));
        }
        None
    }
}

///
/// LRU page buffer caching
///
/// Double linked list with hash table
/// for O(1) access
///
pub struct PageCache {
    file: File,
    capacity: u16, // max # of pages
    size: u64, // Size in bytes of total db file, loaded on startup
    pages: Vec<Vec<u8>>,
}

// TODO: actually implement caching
impl PageCache {
    // TODO: store number of bytes in db info struct on load up,
    // current VERY naive way seeks to end of file
    fn new_page(&mut self) -> Result<PageId> {
        let buffer = PageData::new();
        let len = self.file.seek(SeekFrom::End(0))?;
        assert_eq!(len % PAGE_SIZE as u64, 0);

        let pid = (len / PAGE_SIZE as u64) as PageId;
        self.commit_page(pid, &buffer);

        Ok(pid)
    }

    ///
    /// Returns cached page buffer wrapped as PageData
    /// or requests the buffer from disk through connection
    /// object, updates cache and returns it
    ///
    fn get_page(&mut self, pid: PageId) -> Result<PageData> {
        let mut data = PageData::new();
        let offs = pid as u64 * PAGE_SIZE as u64;
        self.file.read_exact_at(data.as_mut_slice(), offs)?;

        Ok(data)
    }

    fn commit_page(&mut self, pid: PageId, data: &PageData) -> Result<()> {
        let offs = pid as u64 * PAGE_SIZE as u64;
        self.size = self.size.max(offs + PAGE_SIZE as u64);
        // self.size = self.size.max((pid*2) as u64 * PAGE_SIZE as u64);
        self.file.write_all_at(data.as_slice(), offs)?;

        Ok(())
    }
}

///
/// User interface object, abstraction
/// of db operations.
///
pub struct Connection {
    access: BTree,
    pcache: PageCache,
    metadata: MetaData,
}

impl Connection {
    pub fn open(db_path: &Path) -> Result<Connection> {
        // Try intiializing database
        let (file, meta) = if let Ok(file) = File::options().read(true).write(true).open(db_path) {
            let mut buffer = vec![0u8; METADATA_SIZE];
            file.read_exact_at(buffer.as_mut_slice(), 0)?;
            let meta: MetaData =
                bincode::decode_from_slice(buffer.as_mut_slice(), BINCODE_CONFIG)?.0;

            info!("Loaded db metadata: {:#?}", meta);
            (file, meta)
        } else {
            let file = File::options()
                .create(true)
                .read(true)
                .write(true)
                .open(db_path)?;

            let meta = MetaData {
                magic: MAGIC,
                size: PAGE_SIZE as u64,
                root: 0,
                height: 0,
            };

            let mut buffer = vec![0u8; PAGE_SIZE];
            bincode::encode_into_slice(&meta, &mut buffer[..METADATA_SIZE], BINCODE_CONFIG)?;

            file.write_all_at(buffer.as_slice(), 0)?;

            (file, meta)
        };

        Ok(Connection {
            access: BTree::initialize(&meta),
            pcache: PageCache {
                file,
                capacity: CACHE_CAPACITY,
                size: meta.size,
                pages: Vec::new(),
            },
            metadata: meta,
        })
    }

    fn commit_metadata(&mut self) -> Result<()> {
        self.metadata = MetaData {
            height: self.access.height,
            root: self.access.root,
            size: self.pcache.size,
            ..self.metadata
        };

        let mut buffer = vec![0u8; METADATA_SIZE];
        bincode::encode_into_slice(&self.metadata, &mut buffer[..], BINCODE_CONFIG)?;

        self.pcache.file.write_all_at(buffer.as_slice(), 0)?;

        Ok(())
    }

    pub fn put(&mut self, key: &Key, value: &Value) -> Result<()> {
        let result = self.access.btree_insert(&mut self.pcache, key, value);

        self.commit_metadata()?;

        result
    }

    pub fn get(&mut self, key: &Key) -> Result<Value> {
        let result = self.access.btree_get(&mut self.pcache, key);
            
        result
    }
}
