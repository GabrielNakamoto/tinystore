use log::{info,debug};
use std::io::{
    Error as IOError,
    ErrorKind
};
use crate::{
    constants::{
        DBHeader,
        DB_HEADER_SIZE,
        PAGE_SIZE
    },
    pager::Pager
};
use super::node::{
    Node,
    NodeType,
    NodeHeader,
    NODE_HEADER_SIZE,
};
use super::entry::DataEntry;
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

fn get_db_header(pager : &mut Pager) -> std::io::Result<DBHeader> {
    let first_page_buffer = pager.get_page(0)?.0;

    Ok(bincode::decode_from_slice(&first_page_buffer[..DB_HEADER_SIZE], bincode::config::standard()).unwrap().0)
}

pub fn get_record(mut key : Vec<u8>, pager : &mut Pager) -> std::io::Result<Vec<u8>> {
    let mut node = search(&key, pager)?;

    // TODO: ensure mutabiliy is correct,
    // for example since we arent updating the file in
    // this function it should be all immutable slices of the page

    for i in 0..node.header.items_stored {
        match node.decode_data_entry(i as usize)? {
            DataEntry::Leaf(entry_key, value) => {
                // debug!("{:?} / {:?}", entry_key, key);
                if entry_key == key {
                    return Ok(value);
                }
            },
            _ => {
                // Error
                return Err(IOError::new(ErrorKind::Other, "Got internal entry"));
            }
        };
    }
    return Err(IOError::new(ErrorKind::Other, "Couldn't find entry"));
}

pub fn search(key : &Vec<u8>, pager : &mut Pager) -> std::io::Result<Node> {
    let root_page_id = get_db_header(pager)?.root_node as u32;
    let mut cur_page_id = root_page_id;

    while true {
        debug!("Searching node at page id: {}", cur_page_id);
        let node = Node::deserialize(cur_page_id, pager)?;

        match node.header.node_type {
            NodeType::Internal => {
                let mut found = false;
                for i in 0..node.header.items_stored {
                    let entry = node.decode_data_entry(i as usize)?;
                    if let DataEntry::Internal(entry_key, child_page) = entry {
                        if entry_key > *key {
                            cur_page_id = child_page;
                            found = true;
                            break;
                        }
                    };
                }
                if ! found {
                    cur_page_id = node.header.rightmost_child;
                }
            },
            NodeType::Leaf => {
                // Make sure the key is actually in this node
                return Ok(node);
            }
        }
    }
    // TODO: make this an Err
    // THIS IS TOTALLY WRONG
    return Err(IOError::new(ErrorKind::Other, "oh no"));
}

pub fn insert_record(mut key : Vec<u8>, mut value : Vec<u8>, pager : &mut Pager) -> std::io::Result<()> {
    // TODO: handle bincode results

    let mut node = search(&key, pager)?;
    // This is wrong way to check for overflow

    // Overflowed
    if node.header.free_space_end - node.header.free_space_start < (key.len() + value.len()) as u32 {
        info!("Node at id: {} overflowed", node.page_id);
        split_node(&mut node, pager);
    }

    let data_entry = DataEntry::Leaf(key, value);
    node.insert_data_entry(&data_entry);

    // Request changes to page cache
    // TODO: Look at buffered writes
    pager.save_page(&node.page_buffer.to_vec(), Some(node.page_id));
    Ok(())
}

// 
// Insert keys in increasing order (maybe just sort the offset ptrs)
// When there is no more room for a new key, move it to a new 
// node to the right and move the current greatest key from source
// node up to make a new one
//
pub fn split_node(node : &mut Node, pager : &mut Pager) -> std::io::Result<()> {
    // let src_header = self.get_header();
    // move the second half of the values to the new node
    let mut top_page_buffer = Pager::allocate_page_buffer();
    let mut right_page_buffer = Pager::allocate_page_buffer();

    // assume rptrs are sorted in order of increasing keys
    let tm = ((node.header.items_stored + 1) / 2) as usize;
    // only include split entry if leaf node
    let to_move = if node.header.node_type == NodeType::Leaf { tm } else { tm - 1 };

    // Create right node and move pointers and records
    // +1 cause the first value gets moved up??

    let split_entry = node.decode_data_entry((node.header.items_stored as usize) - to_move)?;

    {
        // TODO: Handle new offsets
        let free_space_start = NODE_HEADER_SIZE + (4*to_move);

        // Data entries, removes each entry from source node at same time
        let mut free_space_end = PAGE_SIZE;
        for i in (node.header.items_stored as usize)-to_move..node.header.items_stored as usize { 
            let record = node.decode_data_entry(i)?;
            let new_free_space_end = free_space_end - record.size() as usize;

            record.encode(&mut right_page_buffer[new_free_space_end..free_space_end]);
            free_space_end = new_free_space_end;

            node.remove_data_entry(i)?;
        }

        // Header
        let right_node_header = NodeHeader::get(
            node.header.node_type.clone(),
            (NODE_HEADER_SIZE + 4*to_move) as u32,
            free_space_end as u32,
            to_move as u32);

        info!("Split right node: {:#?}", right_node_header);

        bincode::encode_into_slice(
            &right_node_header,
            &mut right_page_buffer[..NODE_HEADER_SIZE],
            bincode::config::standard());

        // Offset array
        bincode::encode_into_slice(
            &node.offsets_array[(node.header.items_stored as usize)-to_move..],
            &mut right_page_buffer[NODE_HEADER_SIZE..NODE_HEADER_SIZE+(4*to_move)],
            bincode::config::standard());

    }

    let right_id = pager.save_page(&right_page_buffer.to_vec(), None)? as u32;

    // Create upper node and move the split pointer
    {
        let entry_size = split_entry.size();
        // Header
        let mut top_node_header = NodeHeader::get(
            NodeType::Internal, // has to be internal cause moving up
            NODE_HEADER_SIZE as u32+4,
            (PAGE_SIZE as u32) - entry_size,
            1);
        top_node_header.rightmost_child = right_id;

        info!("Split top node: {:#?}", top_node_header);

        bincode::encode_into_slice(
            &top_node_header,
            &mut top_page_buffer[..NODE_HEADER_SIZE],
            bincode::config::standard());

        // Has to be internal node,
        // So if the current node is leaf than we have to 

        // Offset array
        // bincode::encode

        // Split entry
        let offset = top_node_header.free_space_end as usize;
        split_entry.encode(&mut top_page_buffer[offset..]);
        // Node::encode_data_entry(&mut top_page_buffer[offset..], &split_entry);
    }

    let parent_id = pager.save_page(&top_page_buffer.to_vec(), None)? as i32;

    let mut db_header = get_db_header(pager)?;
    if db_header.root_node as u32 == node.page_id {
        info!("New root node at page id: {}", parent_id);
        db_header.root_node = parent_id as u16;
        let mut encoded_header : Vec<u8> = vec![0u8; DB_HEADER_SIZE];
        bincode::encode_into_slice(
            &db_header,
            encoded_header.as_mut_slice(),
            bincode::config::standard());
        pager.save_page(&encoded_header, Some(0));

        info!("Updated db header: {:#?}", get_db_header(pager)?);
    }

    node.header.items_stored -= tm as u32;
    node.header.parent = parent_id;
    node.encode_header();
    pager.save_page(&node.page_buffer, Some(node.page_id));

    Ok(())
}
