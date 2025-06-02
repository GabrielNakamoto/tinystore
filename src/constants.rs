use bincode::{Decode, Encode};

pub const DB_HEADER_SIZE : usize = 10;
pub const PAGE_SIZE : usize = 4096;

#[derive(Decode, Encode)]
pub struct DBHeader {
    magic_numbers : [u8; 10]
}

impl DBHeader {
    pub fn get() -> Self {
        DBHeader {
            magic_numbers : [84, 105, 110, 121, 32, 83, 116, 111, 114, 101]
        }
    }
}
