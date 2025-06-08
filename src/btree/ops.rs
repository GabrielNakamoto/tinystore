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
    // info!("Couldn't finda entry at node: {}", node.page_id);
    return Err(IOError::new(ErrorKind::Other, "Couldn't find entry"));
}

pub fn search(key : &Vec<u8>, pager : &mut Pager) -> std::io::Result<Node> {
    let db_header = get_db_header(pager)?;
    // info!("Search db header: {:#?}", db_header);
    let root_page_id = db_header.root_node as u32;
    let mut cur_page_id = root_page_id;

    // info!("Starting b+tree search at root id: {}", root_page_id);
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

pub fn insert_leaf_data(mut key : Vec<u8>, mut value : Vec<u8>, pager : &mut Pager) -> std::io::Result<()> {
    // TODO: handle bincode results

    let mut node = search(&key, pager)?;

    let data_entry = DataEntry::Leaf(key.clone(), value.clone());

    insert_entry(&data_entry, &mut node, pager)
    // Request changes to page cache
    // TODO: Look at buffered writes
}

fn insert_entry(entry: &DataEntry, node: &mut Node, pager: &mut Pager) -> std::io::Result<()> {
    match node.insert_data_entry(entry) {
        InsertResult::NeedsSplit => {
            split_node(node, pager);

            let mut new_node = search(&entry.key(), pager)?;
            insert_entry(entry, &mut new_node, pager)
        },
        _ => {
            pager.save_page(&node.page_buffer, Some(node.page_id));
            Ok(())
        }
    }
}

// Todo check for subsequent splits
// Also: 
pub fn split_node(node : &mut Node, pager : &mut Pager) -> std::io::Result<()> {
    info!("Splitting node at page id: {}", node.page_id);

    let split_point = ((node.header.items_stored + 1) / 2) as usize;
    // If leaf we keep median record in left node, otherwise noone keeps it
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

    if node.header.node_type == NodeType::Internal {
        // Remove median record
        node.remove_data_entry(node.header.items_stored as usize - 1);
    }

    let mut parent_node = if node.header.parent != -1 {
        Node::deserialize(node.header.parent as u32, pager)?
    } else {
        info!("Creating new parent node for page id: {}", node.page_id);
        Node::new(NodeType::Internal, pager)
    };

    let child_ptr = if node.header.parent == -1 || node.page_id == parent_node.header.rightmost_child {
        parent_node.header.rightmost_child = right_node.page_id;
        parent_node.encode_header();

        node.page_id
    } else {
        right_node.page_id
    };

    let new_parent = node.header.parent == -1;
    // parent_node.insert_data_entry(&split_entry);
    node.header.parent = parent_node.page_id as i32;
    right_node.header.parent = parent_node.page_id as i32;
    node.encode_header();
    right_node.encode_header();

    pager.save_page(&right_node.page_buffer, Some(right_node.page_id));
    pager.save_page(&parent_node.page_buffer, Some(parent_node.page_id));
    pager.save_page(&node.page_buffer, Some(node.page_id));

    let mut db_header = get_db_header(pager)?;
    if db_header.root_node as u32 == node.page_id {
        info!("New root node at page id: {}", parent_node.page_id);
        db_header.root_node = parent_node.page_id as u16;

        // Update 0 nodes header!
        let mut file_first_node = Node::deserialize(0, pager)?;
        bincode::encode_into_slice(
            &db_header,
            &mut file_first_node.page_buffer[..DB_HEADER_SIZE],
            bincode::config::standard());
        pager.save_page(&file_first_node.page_buffer, Some(0));
    }

    // if new_parent {
    //     info!("Split left node: id: {}, {:#?}", node.page_id, node.header);
    //     info!("Split right node: id: {}, {:#?}", right_node.page_id, right_node.header);
    //     info!("New parent node: id: {}, {:#?}", parent_node.page_id, parent_node.header);
    //     assert_eq!(parent_node.page_id, get_db_header(pager)?.root_node as u32);
    // }

    let split_entry = DataEntry::Internal(split_key, child_ptr);
    insert_entry(&split_entry, &mut parent_node, pager); // somethings going wrong here

    // pager.save_page(&parent_node.page_buffer, Some(parent_node.page_id));


    {
        for i in 0..parent_node.header.items_stored {
            let entry = parent_node.decode_data_entry(i as usize).unwrap();
            if let DataEntry::Internal(key, child) = entry {
                // info!("Parent node entry {}: ({:?}, {})", i, key, child);
                let child_node = Node::deserialize(child, pager)?;
                for j in 0..child_node.header.items_stored {
                    let entry = child_node.decode_data_entry(j as usize).unwrap();
                    assert!(*entry.key() < key, "Invariant broken at parent: {}, child: {}, key: {}", parent_node.page_id, i, j);
                }
            }
        }
        let binding = parent_node.decode_data_entry(parent_node.header.items_stored as usize - 1).unwrap();
        let rightmost_child = Node::deserialize(parent_node.header.rightmost_child, pager)?;
        for j in 0..rightmost_child.header.items_stored {
            let entry = rightmost_child.decode_data_entry(j as usize).unwrap();
            assert!(*entry.key() >= *binding.key());
        }
    }

    // info!("Parent rightmost child: {}", parent_node.header.rightmost_child);

    // Verify children are in order of keys?

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
