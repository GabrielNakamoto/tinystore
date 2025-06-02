use bincode::{Decode, Encode};
// n children
// n-1 keys
#[derive(Encode, Decode, Debug)]
pub enum NodeType {
    Root,
    Internal,
    Leaf
}

pub struct SubTreeRefs {
    pub child_ptrs : Vec<usize>,
    pub keys : Vec<Vec<u8>>
}

pub struct Node {
    pub header: NodeHeader,
    pub subtrees: Option<SubTreeRefs>
}

#[derive(Encode, Decode, Debug)]
pub struct NodeHeader {
    pub magic_numbers : [u8; 4],
    pub node_type : NodeType, // serialized to u32
    pub free_space_start : u32,
    pub free_space_end : u32,
    pub stored_items : u32
}
