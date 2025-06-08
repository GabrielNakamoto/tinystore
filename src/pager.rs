use crate::constants::PAGE_SIZE;
use log::{info,debug};
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

    pub fn next_page_id(&mut self) -> u32 {
        let pos = self.file_handle.seek(SeekFrom::End(0)).unwrap() as u32;
        
        pos / PAGE_SIZE as u32
    }

    pub fn get_page(&mut self, page_id : u32) -> std::io::Result<(Vec<u8>, usize)> {
        let mut page_buffer = Self::allocate_page_buffer();
        self.file_handle.seek(SeekFrom::Start((page_id * PAGE_SIZE as u32) as u64));
        let bytes_read = self.file_handle.read(&mut page_buffer)?;
        // Dont do this:
        self.file_handle.rewind();

        Ok((page_buffer, bytes_read))
    }

    pub fn save_page(&mut self, page_buffer : &Vec<u8>, page_id : Option<u32>) -> std::io::Result<usize> {
        match page_id {
            None => { // create new page
                assert_eq!(page_buffer.len(), PAGE_SIZE);

                let pos = self.file_handle.seek(SeekFrom::End(0))? as usize;
                assert_eq!(pos % PAGE_SIZE, 0);

                let new_page_id : usize = pos / PAGE_SIZE;
                // info!("Creating new page at id: {}", new_page_id);

                self.file_handle.write(&page_buffer[..]);
                self.file_handle.rewind();

                Ok(new_page_id)
            },
            Some(id) => { // update page
                debug!("Writing {} bytes to page at id: {}", page_buffer.len(), id);
                self.file_handle.seek(SeekFrom::Start((id * PAGE_SIZE as u32) as u64));
                self.file_handle.write(&page_buffer[..]);
                self.file_handle.rewind();

                Ok(id as usize)
            },
        }
    }
}
