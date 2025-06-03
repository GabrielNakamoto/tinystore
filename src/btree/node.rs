use crate::{
    constants::{
        DB_HEADER_SIZE,
        PAGE_SIZE
    },
    pager::Pager
};
use bincode::{Decode, Encode};

pub const NODE_HEADER_SIZE : usize = 20;
pub const M : usize = 5;


#[derive(Decode, Encode, Debug, PartialEq)]
pub enum NodeType {
    Root,
    Internal,
    Leaf
}

#[derive(Decode, Encode, Debug)]
pub struct NodeHeader {
    magic_numbers : [u8; 4],
    pub node_type : NodeType,
    pub free_space_start : u32,
    pub free_space_end : u32,
    pub items_stored : u32
}

pub struct Node {
    pub page_id : u32,
    pub page_buffer : Vec<u8>,
}

impl Node {
    pub fn new(page_id : u32, pager : &mut Pager) -> std::io::Result<Self> {
        let (page_buffer, bytes_read) = pager.get_page(page_id)?;

        Ok(Node {
            page_id,
            page_buffer
        })
    }


    pub fn get_header(&self) -> std::io::Result<NodeHeader> {
        let header_start = if self.page_id == 0 { DB_HEADER_SIZE } else { 0 } as usize;
        let header_slice = &self.page_buffer[header_start..header_start + NODE_HEADER_SIZE];

        let (mut node_header, bytes_decoded) : (NodeHeader, usize) = bincode::decode_from_slice(
            header_slice,
            bincode::config::standard()).unwrap();

        Ok(node_header)
    }

    pub fn get_record_pointers(&self) -> std::io::Result<Vec<u32>> {
        let node_header = self.get_header()?;

        let header_start = if self.page_id == 0 { DB_HEADER_SIZE } else { 0 } as usize;
        let record_start = header_start + NODE_HEADER_SIZE;

        let records_slice = &self.page_buffer[record_start..node_header.free_space_start as usize];

        let mut offsets : Vec<u32> = Vec::with_capacity(node_header.items_stored as usize);

        for i in 0..node_header.items_stored {
            let rptr_start_offset = (i as usize) * 4;

            let offset : u32 = bincode::decode_from_slice(
                &records_slice[rptr_start_offset..rptr_start_offset+4],
                bincode::config::standard()).unwrap().0;

            offsets.push(offset);
        }

        Ok(offsets)
    }

    // 
    // Insert keys in increasing order (maybe just sort the offset ptrs)
    // When there is no more room for a new key, move it to a new 
    // node to the right and move the current greatest key from source
    // node up to make a new one
    //
    pub fn split(&mut self, pager : &mut Pager) -> std::io::Result<Node> {
        // move the second half of the values to the new node
        let mut top_page_buffer = Pager::allocate_page_buffer();
        let mut right_page_buffer = Pager::allocate_page_buffer();

        // assume rptrs are sorted in order of increasing keys
        let rptrs = self.get_record_pointers()?;
        let to_move = (rptrs.len() + 1) / 2;

        // Create right node and move pointers and records
        // +1 cause the first value gets moved up??
        for ptr in &rptrs[rptrs.len()-(to_move as usize)..] {
        }

        // Create upper node and move the split pointer

        // Remove pointers and records from source node

        // Update source node metadata and records


        Ok(Node {
            page_id : 10,
            page_buffer : vec![0u8; 10],
        })
    }

    // make find node function, returns the page id of the leaf node where
    // the key belongs
    pub fn find_node(key : &Vec<u8>, pager : &mut Pager) -> std::io::Result<Node> {
        let mut cur_page_id = 0; // start at root 

        while true {
            let mut node = Node::new(cur_page_id, pager)?;
            let node_header = node.get_header()?;
            // let (mut page_buffer, bytes_read) = pager.get_page(0)?;
            // let (mut node_header, bytes_decoded) : (NodeHeader, usize) = bincode::decode_from_slice(
            //     &page_buffer[DB_HEADER_SIZE..],
            //     bincode::config::standard()).unwrap();

            match node_header.node_type {
                NodeType::Root => {
                },
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
        Ok(Node::new(0, pager)?)
    }
}

impl NodeHeader {
    pub fn get(node_type : NodeType, free_space_start : u32, free_space_end : u32, items_stored : u32) -> Self {
        Self {
            magic_numbers: [80, 65, 71, 69],
            node_type,
            free_space_start,
            free_space_end,
            items_stored
        }
    }
}
