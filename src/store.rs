use log::info;
use bincode::{
    Encode,
    Decode,
    config::BigEndian
};
use anyhow::{anyhow, Result};
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::fs::File;


type PageId = u32;

/// Index or 'id' of an item and its corresponding offset, page local
///
/// Starts at 0 at back of page and increases towards offset array
type ItemPtr = usize; 
type Key = Vec<u8>;
type Value = Vec<u8>;

const BINCODE_CONFIG: bincode::config::Configuration<BigEndian> = bincode::config::standard().with_big_endian();
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

#[derive(Encode, Decode)]
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
    buf: [u8; PAGE_SIZE]
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

impl PageData {
    fn new() -> PageData {
        PageData {
            buf: [0u8; PAGE_SIZE]
        }
    }

    pub fn search(&self, target: &Key) -> Option<(Key, Value)> {
        for i in 0..self.get_n_items() {
            let (key, value) = self.get_item(i);
            if key == *target {
                return Some((key, value));
            }
        }
        None
    }

    pub fn lin_find_place(&self, key: &Key) -> ItemPtr {
        let n = self.get_n_items();
        for i in 0..n {
            if ! self.gt_entry(key, i) {
                return i;
            }
        }

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

    pub fn insert_item(&mut self, ip: ItemPtr, key: &Key, value: &Value) -> bool {
        let n_items = self.get_n_items();
        let size = self.get_size();
        let kl = key.len();
        let vl = value.len();
        let il = 4 + kl + vl;

        if PAGE_SIZE - PAGE_HEADER_SIZE >= (n_items * 2) + size + il + 2 {
            // Shift greater items towards header,
            // offsets will decrease
            for i in (ip..n_items).rev() {
                self.set_offs(i+1, self.get_offs(i) - il);
            }

            let offs = if ip == 0 {
                PAGE_SIZE - il
            } else {
                self.get_offs(ip-1) - il
            };

            if ip != n_items {
                let start = self.get_offs(n_items);
                self.buf.copy_within(start..offs+il, start-il);
            }

            self.set_offs(ip, offs);
            self.set_u16(offs, kl as u16);
            self.set_u16(offs+2, vl as u16);

            let offs = offs+4;
            self.buf[offs..offs+kl].copy_from_slice(key.as_slice());
            self.buf[offs+kl..offs+kl+vl].copy_from_slice(value.as_slice());

            self.set_n_items(n_items+1);

            true
        } else {
            false
        }
    }

    pub fn get_item(&self, ip: ItemPtr) -> (Key, Value) {
        let offs = self.get_offs(ip);
        let kl = self.get_u16(offs) as usize;
        let vl = self.get_u16(offs+2) as usize;

        let offs = offs+4;
        (
            self.buf[offs..offs+kl].to_vec(),
            self.buf[offs+kl..offs+kl+vl].to_vec()
        )
    }

    fn get_size(&self) -> usize {
        let mut size: usize = 0;
        for i in 0..self.get_n_items() {
            let (key, value) = self.get_item(i);
            size += key.len() + value.len();
        }

        size
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
        self.buf[offs..offs+2].copy_from_slice(&data.to_be_bytes());
    }

    fn get_u16(&self, offs: usize) -> u16 {
        u16::from_be_bytes(self.buf[offs..offs+2].try_into().unwrap())
    }

    fn set_u32(&mut self, offs: usize, data: u32) {
        self.buf[offs..offs+4].copy_from_slice(&data.to_be_bytes());
    }

    fn get_u32(&self, offs: usize) -> u32 {
        u32::from_be_bytes(self.buf[offs..offs+4].try_into().unwrap())
    }
}

struct BTree {
    root: PageId,
    height: u16
}


impl BTree {
    pub fn initialize(meta: &MetaData) -> BTree {
        BTree {
            root: meta.root,
            height: meta.height
        }
    }

    pub fn put(&mut self, io: &mut IOManager, key: &Key, value: &Value) -> Result<()> {
        let (pid, page) = if self.root == 0 { // First page, leaf root
            self.root = 1;
            self.height = 1;

            (1, self.allocate_leaf_page(key, value))
        } else {
            let pid = self.search(io, key)?;
            let mut page = io.get_page(pid)?;
            let ip = page.lin_find_place(key);

            if ! page.insert_item(ip, key, value) {
                // TODO: Handle overflow
                return Err(anyhow!("Failed to insert item"));
            }

            (pid, page)
        };

        io.commit_page(pid, &page);
        Ok(())
    }

    fn allocate_leaf_page(&self, key: &Key, value: &Value) -> PageData {
        let mut page = PageData::new();
        page.insert_item(0, key, value);

        page
    }

    fn allocate_internal_page(&self, key: &Key, lchild: PageId, rchild: PageId) -> PageData {
        let mut page = PageData::new();
        page.insert_item(0, key, &lchild.to_be_bytes().to_vec());
        page.insert_item(1, &vec![], &rchild.to_be_bytes().to_vec());

        page
    }

    fn search(&self, io: &mut IOManager, key: &Key) -> Result<PageId> {
        let mut pid: PageId = self.root;
        let mut h = self.height;

        // Descend through tree
        while h > 0 {
            h -= 1;

            if h == 0 { // At leaf node
                return Ok(pid);
            } else { // Find child ptr
                let page = io.get_page(pid)?;
                let ip = page.lin_find_place(key);
                let (_, child) = page.get_item(ip);
                pid = u32::from_be_bytes(child.as_slice().try_into().unwrap());
            }
        }

        Err(anyhow!("Couldn't find leaf node"))
    }

    fn split_leaf_page() {
    }

    fn split_internal_page() {
    }
}

struct IOManager {
    file: File,
    size: u64 // # of bytes stored in db file
}

///
/// Disk level IO abstraction
///
/// Used by page cache, transaction commiting and
/// logging.
///
impl IOManager {
    pub fn initialize(file: File, size: u64) -> IOManager {
        IOManager {
            file,
            size
        }
    }

    pub fn get_size(&self) -> u64 {
        self.size
    }

    fn commit_page(&mut self, pid: PageId, data: &PageData) {
        let offs = pid as u64 * PAGE_SIZE as u64;
        self.size = self.size.max((pid*2) as u64 * PAGE_SIZE as u64);
        self.file.write_all_at(data.as_slice(), offs);
    }

    fn get_page(&self, pid: PageId) -> Result<PageData> {
        let mut data = PageData::new();
        let offs = pid as u64 * PAGE_SIZE as u64;
        self.file.read_exact_at(data.as_mut_slice(), offs)?;

        Ok(data)
    }
}

/// 
/// User interface object, abstraction
/// of db operations.
///
pub struct Connection {
    io: IOManager,
    access: BTree
}

impl Connection {
    pub fn open(db_path: &Path) -> Result<Connection> {
        // Try intiializing database
        let (file, meta) = if let Ok(file) = File::options().read(true).write(true).open(db_path) {
            let mut buffer = vec![0u8; METADATA_SIZE];
            let meta: MetaData = bincode::decode_from_slice(buffer.as_mut_slice(), BINCODE_CONFIG)?.0;

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
                height: 0
            };

            let mut buffer = vec![0u8; PAGE_SIZE];
            bincode::encode_into_slice(&meta, &mut buffer[..METADATA_SIZE], BINCODE_CONFIG)?;

            file.write_all_at(buffer.as_slice(), 0)?;

            (file, meta)
        };
        
        Ok(Connection {
            io: IOManager::initialize(file, meta.size),
            access: BTree::initialize(&meta)
        })
    }

    pub fn put(&mut self, key: &Key, value: &Value) -> Result<()> {
        self.access.put(&mut self.io, key, value)?;
        Ok(())
    }
}

