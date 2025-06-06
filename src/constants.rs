use bincode::{Decode, Encode};

pub const DB_HEADER_SIZE : usize = 12;
pub const PAGE_SIZE : usize = 4096;

#[derive(Decode, Encode, Debug)]
pub struct DBHeader {
    magic_numbers : [u8; 10],
    pub root_node : u16
}

impl DBHeader {
    pub fn get() -> Self {
        DBHeader {
            magic_numbers : [84, 105, 110, 121, 32, 83, 116, 111, 114, 101],
            root_node: 0
        }
    }
}
