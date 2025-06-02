// magic number + node type + free space start / end offsets + # of items
//  32 bits         32 bits           32 * 2 bytes                32 bits
pub const NODE_HEADER_SIZE : u32 = 32;

pub const NODE_MAGIC_NUMBERS : [u8; 4] = [80, 65, 71, 69];
