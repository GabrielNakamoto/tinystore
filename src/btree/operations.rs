use log::info;
use bincode::config;
use super::{
    constants,
    node::{
        Node,
        NodeHeader,
        NodeType
    }
};
use crate::pager::{
    Pager,
    constants::{
        DB_HEADER_SIZE,
        PAGE_SIZE
    }
};


pub fn create_leaf_node() -> Node {
    Node {
        header: NodeHeader {
            magic_numbers : constants::NODE_MAGIC_NUMBERS,
            node_type: NodeType::Leaf,
            free_space_start: constants::NODE_HEADER_SIZE,
            free_space_end: PAGE_SIZE,
            stored_items: 0
        },
        subtrees: None
    }
}

pub fn find_node(key : Vec<u8>, pager : &mut Pager) {
    // start at root node
    let mut page_buffer = Pager::allocate_page_buffer();
    let root_id = pager.get_page(page_buffer.as_mut_slice(), 0);

    // deserialize page contents
    let start_offset = DB_HEADER_SIZE as usize;
    let end_offset = (DB_HEADER_SIZE + constants::NODE_HEADER_SIZE) as usize;

    let header_slice = &page_buffer[start_offset..end_offset];

    // TODO: return result idk
    let (node_header, bytes_read) : (NodeHeader, _) = bincode::decode_from_slice(header_slice, config::standard()).unwrap();
    info!("{:#?}", node_header);
}

