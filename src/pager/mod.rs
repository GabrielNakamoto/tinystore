pub mod constants;
pub mod operations;

use std::fs::File;
use std::io::{
    Read,
    Write,
    Seek,
    SeekFrom,
    Error as IOError
};
use std::path::Path;

// Dont know anything about B-Tree
pub struct Pager {
    file : File,
    page_size: usize,
}

impl Pager {
    pub fn new(db_path : &Path) -> Result<Pager, IOError> {
        let file = File::options()
            .read(true)
            .write(true)
            .create(true)
            .open(db_path)?;

        let mut pager = Pager {
            file,
            page_size: constants::PAGE_SIZE as usize
        };

        Ok(pager)
    }
}
// struct PageHeader {
//     magic:  [u8; 4],
//     ntype: NodeType,
//     free_start_offset: PageSpace,
//     free_end_offset: PageSpace,
//     n_elements: PageSpace
// }
