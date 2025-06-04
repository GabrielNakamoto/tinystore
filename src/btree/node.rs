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
    // Root,
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

// Node structure :
//
//      node header
//      offset array
//      data (records or seperator keys)
//

// pub struct Node {
//     pub page_id : u32,
//     pub page_buffer : Vec<u8>,
// }

pub struct Node {
    pub page_id : u32,
    pub page_buffer : Vec<u8>,
    pub header: NodeHeader,
    pub offsets_array : Vec<u32>
}

pub enum DataEntry {
    Internal(Vec<u8>, u32), // Key, Child ptr (page_id)
    Leaf(Vec<u8>, Vec<u8>), // Key, Value
}

impl Node {
    pub fn deserialize(page_id : u32, pager : &mut Pager) -> std::io::Result<Self> {
        let (page_buffer, bytes_read) = pager.get_page(page_id)?;

        let header = Self::get_header(page_id, &page_buffer)?;
        let offsets_array = Self::get_offsets_array(&header, page_id, &page_buffer)?;

        Ok(Node {
            page_id,
            page_buffer,
            header,
            offsets_array
        })
    }

    fn get_header(page_id : u32, page_buffer : &Vec<u8>) -> std::io::Result<NodeHeader> {
        let header_start = if page_id == 0 { DB_HEADER_SIZE } else { 0 } as usize;
        let header_slice = &page_buffer[header_start..header_start + NODE_HEADER_SIZE];

        let (mut node_header, bytes_decoded) : (NodeHeader, usize) = bincode::decode_from_slice(
            header_slice,
            bincode::config::standard()).unwrap();

        Ok(node_header)
    }

    fn get_offsets_array(header : &NodeHeader, page_id : u32, page_buffer : &Vec<u8>) -> std::io::Result<Vec<u32>> {
        // let node_header = self.get_header()?;

        let header_start = if page_id == 0 { DB_HEADER_SIZE } else { 0 } as usize;
        let array_start = header_start + NODE_HEADER_SIZE;

        let array_slice = &page_buffer[array_start..header.free_space_start as usize];

        let mut offsets_array : Vec<u32> = Vec::with_capacity(header.items_stored as usize);

        for i in 0..header.items_stored {
            let offset_ptr = (i as usize) * 4;

            let offset : u32 = bincode::decode_from_slice(
                &array_slice[offset_ptr..offset_ptr+4],
                bincode::config::standard()).unwrap().0;

            offsets_array.push(offset);
        }

        Ok(offsets_array)
    }

    pub fn decode_data_entry(&self, entry_id : usize) -> std::io::Result<DataEntry> {
        // TODO: handle out of range error
        let entry_offset = self.offsets_array[entry_id] as usize;
        let key_len : u32 = bincode::decode_from_slice(
            &self.page_buffer[entry_offset..entry_offset+4],
            bincode::config::standard()).unwrap().0;

        match self.header.node_type {
            NodeType::Internal => {
                let page_id : u32 = bincode::decode_from_slice(
                    &self.page_buffer[entry_offset+4..entry_offset+8],
                    bincode::config::standard()).unwrap().0;
                let record_key = &self.page_buffer[entry_offset+8..entry_offset+8+(key_len as usize)];

                Ok(DataEntry::Internal(record_key.to_vec(), page_id))
            },
            NodeType::Leaf => {
                let value_len : u32 = bincode::decode_from_slice(
                    &self.page_buffer[entry_offset+4..entry_offset+8],
                    bincode::config::standard()).unwrap().0;

                let value_start = entry_offset+8+(key_len as usize);
                let record_key = &self.page_buffer[entry_offset+8..value_start];
                let record_value = &self.page_buffer[value_start..value_start+(value_len as usize)];

                // TODO: Make these slices not vecs?
                Ok(DataEntry::Leaf(record_key.to_vec(), record_value.to_vec()))
            }
        }
        // Ok((record_key, record_value))
    }

    pub fn encode_data_entry(entry_slice : &mut [u8], entry : &DataEntry) {
        match entry {
            DataEntry::Leaf(key, value) => {
                bincode::encode_into_slice(
                    key.len() as u32, &mut entry_slice[..4], bincode::config::standard());
                bincode::encode_into_slice(
                    value.len() as u32, &mut entry_slice[4..8], bincode::config::standard());

                let value_start = 8 + key.len();

                &mut entry_slice[8..value_start].copy_from_slice(&key[..]);
                &mut entry_slice[value_start..value_start+value.len()].copy_from_slice(&value[..]);
            },
            DataEntry::Internal(key, page_id) => {
                bincode::encode_into_slice(
                    key.len() as u32, &mut entry_slice[..4], bincode::config::standard());
                bincode::encode_into_slice(
                    page_id, &mut entry_slice[4..8], bincode::config::standard());

                &mut entry_slice[8..8+key.len()].copy_from_slice(&key[..]);
            }
        }
    }

    // 
    // Insert keys in increasing order (maybe just sort the offset ptrs)
    // When there is no more room for a new key, move it to a new 
    // node to the right and move the current greatest key from source
    // node up to make a new one
    //
    pub fn split(&mut self, pager : &mut Pager) -> std::io::Result<Node> {
        // let src_header = self.get_header();
        // move the second half of the values to the new node
        let mut top_page_buffer = Pager::allocate_page_buffer();
        let mut right_page_buffer = Pager::allocate_page_buffer();

        // assume rptrs are sorted in order of increasing keys
        // let rptrs = self.get_offsets_array()?;
        let to_move = ((self.header.items_stored + 1) / 2) as usize;

        // Create right node and move pointers and records
        // +1 cause the first value gets moved up??

        {
            let free_space_start = NODE_HEADER_SIZE + (4*to_move);

            // let right_node_header = NodeHeader::get(
            //     src_header.node_type,
            //     NODE_HEADER_SIZE as u32 + (4*to_move),
            //     PAGE_SIZE-
    
            let mut free_space_end = PAGE_SIZE;
            for i in (self.header.items_stored as usize)-to_move..self.header.items_stored as usize { 
                // free_space_end -= 8 + 
                let record = self.decode_data_entry(i)?;
                let new_free_space_end = free_space_end - match &record {
                    DataEntry::Leaf(key, value) => {
                        8 + key.len() + value.len()
                    },
                    DataEntry::Internal(key, page_id) => {
                        8 + key.len()
                    }
                };

                Node::encode_data_entry(&mut right_page_buffer[new_free_space_end..free_space_end], &record);
                free_space_end = new_free_space_end;
            }
        }

        // Create upper node and move the split pointer

        // Remove pointers and records from source node

        // Update source node metadata and records

        pager.save_page(top_page_buffer.to_vec(), None);
        pager.save_page(right_page_buffer.to_vec(), None);

        Ok(Node::deserialize(0, pager)?)
    }
}

