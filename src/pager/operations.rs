use super::Pager;
use super::constants;
use std::fs::File;
use std::io::{
    Read,
    Write,
    Seek,
    SeekFrom,
    Error as IOError
};
use std::path::Path;

impl Pager {
    pub fn allocate_page_buffer() -> Vec<u8> {
        vec![0u8; constants::PAGE_SIZE]
    }

    pub fn save_page(&mut self, payload : &[u8], page_id : Option<i32>) -> std::io::Result<usize> {
        let bytes_wrote = match page_id {
            None => { // new page
                0
            },
            Some(0) => { // meta / root page
                let mut buffer = Self::allocate_page_buffer();

                // serialize header
                let header_slice = &mut buffer[..constants::DB_HEADER_SIZE];

                let mut header = vec![0u8; constants::DB_HEADER_SIZE];
                &header[..constants::MAGIC_HEADER.len()].copy_from_slice(&constants::MAGIC_HEADER[..]);

                header_slice.copy_from_slice(&header[..]);

                // TODO: serialize root node

                self.file.write(&buffer)?
            },
            Some(page) => { // edit page
                0
            }
        };

        Ok(bytes_wrote)
    }

    // TODO: cache recently requested pages and check cache first
    pub fn get_page(&mut self, page_buffer : &mut [u8], page_id : i32) -> std::io::Result<usize> {
        self.file.seek(SeekFrom::Start(page_id as u64 * constants::PAGE_SIZE as u64));
        let bytes_read = self.file.read(page_buffer)?;
        self.file.rewind();

        Ok(bytes_read)
    }
}
