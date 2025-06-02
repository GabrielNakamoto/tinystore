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
        vec![0u8; constants::PAGE_SIZE as usize]
    }

    pub fn save_page(&mut self, payload : &mut [u8], page_id : Option<i32>) -> std::io::Result<usize> {
        let bytes_wrote = match page_id {
            None => { // new page
                0
            },
            Some(0) => { // meta / root page
                let mut buffer = Self::allocate_page_buffer();

                // serialize db header
                let header_slice = &mut buffer[..constants::DB_HEADER_SIZE as usize];

                let mut header = vec![0u8; constants::DB_HEADER_SIZE as usize];
                &header[..constants::MAGIC_HEADER.len()].copy_from_slice(&constants::MAGIC_HEADER[..]);

                header_slice.copy_from_slice(&header[..]);

                // TODO: serialize root node
                let buffer_payload_slice = &mut buffer[(constants::DB_HEADER_SIZE) as usize..];

                // only take slice of remaining space 
                let payload_slice = &mut payload[..(constants::PAGE_SIZE-constants::DB_HEADER_SIZE) as usize];

                buffer_payload_slice.copy_from_slice(payload_slice);

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
