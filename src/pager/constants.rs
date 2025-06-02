pub type PageSpace = u32;

pub const PAGE_SIZE : usize = 4096;
pub const DB_HEADER_SIZE : usize = 100;
pub const MAGIC_HEADER : [u8; 10] = [84, 105, 110, 121, 32, 83, 116, 111, 114, 101];
