use crate::constants::PAGE_SIZE;
use std::fs::File;
use std::io::{
    Read,
    Write,
    Seek,
    SeekFrom
};
use std::path::Path;

pub struct Pager {
    file_handle : File,
}

impl Pager {
    pub fn new(db_path : &Path) -> std::io::Result<Pager> {
        let file_handle = File::options()
            .write(true)
            .read(true)
            .create(true)
            .open(db_path)?;
            // .append(true)

        Ok(Pager {
            file_handle,
        })
    }

    pub fn allocate_page_buffer() -> Vec<u8> {
        vec![0u8; PAGE_SIZE as usize]
    }

    pub fn get_page(&mut self, page_id : u32) -> std::io::Result<(Vec<u8>, usize)> {
        let mut page_buffer = Self::allocate_page_buffer();
        self.file_handle.seek(SeekFrom::Start((page_id * PAGE_SIZE as u32) as u64));
        let bytes_read = self.file_handle.read(&mut page_buffer)?;
        // Dont do this:
        self.file_handle.rewind();

        Ok((page_buffer, bytes_read))
    }

    pub fn save_page(&mut self, mut page_buffer : Vec<u8>, page_id : Option<u32>) -> std::io::Result<usize> {
        match page_id {
            None => { // create new page
                return self.file_handle.write(page_buffer.as_mut_slice());
            },
            Some(id) => { // update page
                self.file_handle.seek(SeekFrom::Start((id * PAGE_SIZE as u32) as u64));
                self.file_handle.write(page_buffer.as_mut_slice());
                self.file_handle.rewind();
            },
        };

        Ok(0)
    }
}
