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
    InsertResult
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

    for i in 0..node.header.items_stored {
        match node.decode_data_entry(i as usize)? {
            DataEntry::Leaf(entry_key, value) => {
                debug!("{:?} / {:?}", entry_key, key);
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
    let db_header = get_db_header(pager)?;
    let root_page_id = db_header.root_node as u32;
    let mut cur_page_id = root_page_id;

    debug!("Starting b+tree search at root id: {}", root_page_id);
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

    let data_entry = DataEntry::Leaf(key.clone(), value.clone());
    match node.insert_data_entry(&data_entry) {
        InsertResult::NeedsSplit => {
            split_node(&mut node, pager);

            insert_record(key, value, pager)
        },
        _ => {
            pager.save_page(&node.page_buffer, Some(node.page_id));

            Ok(())
        }
    }
    // Request changes to page cache
    // TODO: Look at buffered writes
}

pub fn split_node(node : &mut Node, pager : &mut Pager) -> std::io::Result<()> {
    // info!("Splitting node at page id: {}", node.page_id);

    let split_point = ((node.header.items_stored + 1) / 2) as usize;
    let to_move = if node.header.node_type == NodeType::Leaf { split_point } else { split_point - 1 };

    // Create right node and move pointers and records
    let split_key = node.decode_data_entry((node.header.items_stored as usize) - to_move)?.key().to_vec();

    let mut right_node = Node::new(node.header.node_type.clone(), pager);
    right_node.encode_header();

    let starting_count = node.header.items_stored as usize;
    for i in 1..=to_move {
        let idx = starting_count - i;
        let record = node.decode_data_entry(idx)?;
        right_node.insert_data_entry(&record);
        node.remove_data_entry(idx)?;
    }
    pager.save_page(&right_node.page_buffer, None);

    let mut parent_node = if node.header.parent != -1 {
        Node::deserialize(node.header.parent as u32, pager)?
    } else {
        info!("Creating new parent node for page id: {}", node.page_id);
        Node::new(NodeType::Internal, pager)
    };

    let child_ptr = if node.header.parent == -1 || node.page_id == parent_node.header.rightmost_child {
        parent_node.header.rightmost_child = right_node.page_id;
        node.page_id
    } else {
        right_node.page_id
    };

    let split_entry = DataEntry::Internal(split_key, child_ptr);
    parent_node.insert_data_entry(&split_entry);

    node.header.parent = parent_node.page_id as i32;
    right_node.header.parent = parent_node.page_id as i32;
    node.encode_header();
    right_node.encode_header();

    pager.save_page(&right_node.page_buffer, Some(right_node.page_id));
    pager.save_page(&parent_node.page_buffer, Some(parent_node.page_id));

    let mut db_header = get_db_header(pager)?;
    if db_header.root_node as u32 == node.page_id {
        info!("New root node at page id: {}", parent_node.page_id);
        db_header.root_node = parent_node.page_id as u16;

        bincode::encode_into_slice(
            &db_header,
            &mut node.page_buffer[..DB_HEADER_SIZE],
            bincode::config::standard());
    }

    pager.save_page(&node.page_buffer, Some(node.page_id));

    // info!("Split left node: {:#?}", node.header);
    // info!("Split right node: {:#?}", right_node.header);

    // for i in 0..parent_node.header.items_stored {
    //     let entry = parent_node.decode_data_entry(i as usize).unwrap();
    //     if let DataEntry::Internal(key, child) = entry {
    //         info!("Parent node entry {}: ({:?}, {})", i, key, child);
    //     }
    // }
    // info!("Parent rightmost child: {}", parent_node.header.rightmost_child);

    // for i in 0..node.header.items_stored {
    //     let entry = node.decode_data_entry(i as usize).unwrap();

    //     info!("[{}] {:?}", node.page_id, entry.key());
    // }
    // for i in 0..right_node.header.items_stored {
    //     let entry = right_node.decode_data_entry(i as usize).unwrap();

    //     info!("[{}] {:?}", right_node.page_id, entry.key());
    // }

    Ok(())
}
