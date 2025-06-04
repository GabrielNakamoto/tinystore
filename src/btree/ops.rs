use std::io::{
    Error as IOError,
    ErrorKind
};
use crate::{
    constants::{
        DB_HEADER_SIZE,
        PAGE_SIZE
    },
    pager::Pager
};
use super::node::{
    Node,
    NodeType,
    NodeHeader,
    DataEntry,
    NODE_HEADER_SIZE,
};
/// only function that should expose inner ds semantics
/// to connection
pub fn initialize_tree(page_buffer : &mut Vec<u8>) -> std::io::Result<()> {
    // Ignore the space allocated for Db header
    let root_slice = &mut page_buffer[DB_HEADER_SIZE..];
    let header_slice = &mut root_slice[..NODE_HEADER_SIZE];

    let mut root_header = NodeHeader::get(NodeType::Leaf, (DB_HEADER_SIZE+NODE_HEADER_SIZE) as u32, PAGE_SIZE as u32, 0);

    bincode::encode_into_slice(root_header, header_slice, bincode::config::standard());

    Ok(())
}

pub fn get_record(mut key : Vec<u8>, pager : &mut Pager) -> std::io::Result<Vec<u8>> {
    let mut node = find_node(&key, pager)?;

    // TODO: ensure mutabiliy is correct,
    // for example since we arent updating the file in
    // this function it should be all immutable slices of the page
    // let node_header = node.get_header()?;
    // let rptrs = node.get_offsets_array()?;
    // let page_buffer = &mut node.page_buffer;

    for i in 0..node.header.items_stored {
        let entry = node.decode_data_entry(i as usize)?;

        match entry {
            DataEntry::Leaf(entry_key, value) => {
                if entry_key == key {
                    return Ok(value);
                }
            },
            _ => {
                // Error
                return Err(IOError::new(ErrorKind::Other, "oh no"));
            }
        };
    }

    Ok(String::from("Error Lmao").into_bytes())
}

pub fn find_node(key : &Vec<u8>, pager : &mut Pager) -> std::io::Result<Node> {
    let mut cur_page_id = 0; // start at root 

    while true {
        let node = Node::deserialize(cur_page_id, pager)?;
        // let mut node = Node::get(cur_page_id, pager)?;
        // let node_header = node.get_header()?;
        // let (mut page_buffer, bytes_read) = pager.get_page(0)?;
        // let (mut node_header, bytes_decoded) : (NodeHeader, usize) = bincode::decode_from_slice(
        //     &page_buffer[DB_HEADER_SIZE..],
        //     bincode::config::standard()).unwrap();

        match node.header.node_type {
            NodeType::Internal => {
                // ptr key ptr key ... ptr key ptr
                // n ptrs, n-1 keys where ptr is a page id
                // these are stored at the end of the free space as well
            },
            NodeType::Leaf => {
                return Ok(node);
            }
        };
        // if node_header.node_type == NodeType::Leaf {
        //     return Ok(cur_page_id)
        // }
    }

    // TODO: make this an Err
    // THIS IS TOTALLY WRONG
    Ok(Node::deserialize(0, pager)?)
}


pub fn insert_record(mut key : Vec<u8>, mut value : Vec<u8>, pager : &mut Pager) -> std::io::Result<()> {
    // TODO: When inserting a new record pointer put the ptr in order of key comparisons?

    // get root
    // TODO: handle bincode results
    let mut node = find_node(&key, pager)?;
    // let mut node_header = node.get_header()?;

    // Overflowed
    if node.header.free_space_end - node.header.free_space_start < (key.len() + value.len()) as u32 {
        let new_node = node.split(pager)?;
    }

    // let mut page_buffer = &mut node.page_buffer;
    let header_start = if node.page_id == 0 { DB_HEADER_SIZE } else { 0 } as usize;

    // for each (key, value)
    // store in metadata:
    // -    offset to start of each record (key, value pair) -> 4 bytes
    //      and length of key and value

    let record_offset = node.header.free_space_end-((key.len() + value.len() + 8) as u32);
    let key_size = key.len() as u32;
    let value_size = value.len() as u32;

    // Update Metadata Section
    let meta_end_offset = node.header.free_space_start as usize;
    let meta_entry_slice = &mut node.page_buffer[meta_end_offset..meta_end_offset+4];
    bincode::encode_into_slice(record_offset, meta_entry_slice, bincode::config::standard());

    // TODO: Store key value sizes in record section? Only store offsets at start

    // Update Record Section
    let mut record_slice =
        &mut node.page_buffer[record_offset as usize..node.header.free_space_end as usize];

    // Update node header
    node.header.free_space_start += 4;
    node.header.free_space_end -= (key.len() + value.len() + 8) as u32;
    node.header.items_stored += 1;

    let data_entry = DataEntry::Leaf(key, value);
    Node::encode_data_entry(&mut record_slice, &data_entry);


    let mut header_slice = &mut node.page_buffer[header_start..header_start+NODE_HEADER_SIZE as usize];

    bincode::encode_into_slice(&node.header, header_slice, bincode::config::standard());

    // Request changes to page cache
    //      Look at buffered writes
    pager.save_page(node.page_buffer.to_vec(), Some(node.page_id));

    // decode child pointers, if this node is a leaf then I can just insert
    // println!("Root header: {:#?}", root_header);

    // search for node / page containing this key
    // update with record metada and values
    Ok(())
}
