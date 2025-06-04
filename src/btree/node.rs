use prev_iter::PrevPeekable;
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


#[derive(Decode, Encode, Debug, Clone, PartialEq)]
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
    pub items_stored : u32,
    pub first_free_block_offset : u16
}

impl NodeHeader {
    pub fn get(node_type : NodeType, free_space_start : u32, free_space_end : u32, items_stored : u32) -> Self {
        Self {
            magic_numbers: [80, 65, 71, 69],
            node_type,
            free_space_start,
            free_space_end,
            items_stored,
            first_free_block_offset: 0
        }
    }
}

pub struct Node {
    pub page_id : u32,
    pub page_buffer : Vec<u8>,
    pub header: NodeHeader,
    pub offsets_array : Vec<u32>
}

#[derive(Decode, Encode, Clone, Copy)]
pub struct FreeBlock {
    pub next_ptr: u16,
    pub total_size: u16
}

pub struct FreeBlockIter<'a> {
    pub parent: &'a Node,
    pub curr: FreeBlock
}

impl<'a> Iterator for FreeBlockIter<'a> {
    type Item = FreeBlock;

    fn next(&mut self) -> Option<Self::Item> {
        None
    }
}

impl<'a> FreeBlockIter<'a> {
    pub fn new(parent : &'a Node) -> Option<Self> {
        let block = parent.first_free_block()?;

        Some(Self {
            parent,
            curr: block
        })
    }
}

pub enum DataEntry {
    Internal(Vec<u8>, u32), // Key, Child ptr (page_id)
    Leaf(Vec<u8>, Vec<u8>), // Key, Value
}

impl Node {
    pub fn first_free_block(&self) -> Option<FreeBlock> {
        if self.header.first_free_block_offset == 0 {
            None
        } else {
            let start = self.header.first_free_block_offset as usize;
            let end = start + 4;

            let block : FreeBlock = bincode::decode_from_slice(
                &self.page_buffer[start..end], bincode::config::standard()).ok().map(|(block, _)| block)?;

            Some(block)
        }
    }

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

    fn get_entry_size(entry : &DataEntry) -> usize {
        match entry {
            DataEntry::Leaf(key, value) => {
                8 + key.len() + value.len()
            },
            DataEntry::Internal(key, page_id) => {
                8 + key.len()
            }
        }
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

    // Optimize this, to batch together removals / updating offsets array
    pub fn remove_data_entry(&mut self, entry_id : usize) -> std::io::Result<()> {
        let entry_offset = self.offsets_array[entry_id] as usize;

        let iter = FreeBlockIter::new(self);

        // Update block chain / page header
        let next_ptr = match iter {
            Some(mut iter) => {
                // TODO: handle option => results
                let mut prev_iter = PrevPeekable::new(iter);
                let mut block_before = prev_iter.find(|block| block.next_ptr > entry_offset as u16).unwrap();

                let ptr = block_before.next_ptr;
                block_before.next_ptr = entry_offset as u16;

                let before_ptr = match prev_iter.prev_peek() {
                    Some(block) => {
                        block.next_ptr
                    },
                    None => {
                        self.header.first_free_block_offset
                    }
                } as usize;

                // Update previous node in linked list to contain ptr to new block
                bincode::encode_into_slice(
                    &block_before,
                    &mut self.page_buffer[before_ptr..before_ptr+4],
                    bincode::config::standard());

                // Return ptr to next block
                ptr
            },
            None => {
                // Add to page header
                self.header.first_free_block_offset = entry_offset as u16;

                // Encode change
                let header_start = if self.page_id == 0 { DB_HEADER_SIZE } else { 0 } as usize;
                bincode::encode_into_slice(
                    &self.header,
                    &mut self.page_buffer[header_start..header_start+NODE_HEADER_SIZE],
                    bincode::config::standard());

                0
            }
        };

        // Create free block
        let block = FreeBlock {
            next_ptr,
            total_size: Self::get_entry_size(&self.decode_data_entry(entry_id)?) as u16
        };
    
        // Encode new block
        bincode::encode_into_slice(
            &block,
            &mut self.page_buffer[entry_offset..entry_offset + 4],
            bincode::config::standard());

        // Update offsets array
        let to_shift = self.header.items_stored as usize-entry_id;

        // Just move entire slice at once
        let shift_start = if self.page_id == 0 { DB_HEADER_SIZE } else { 0 } + NODE_HEADER_SIZE + (to_shift as usize * 4);
        &mut self.page_buffer.copy_within(shift_start..self.header.free_space_start as usize, shift_start-1);

        self.header.free_space_start -= 4;

        // Save changes?
        Ok(())
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
        let to_move = (((self.header.items_stored + 1) / 2) - 1) as usize;

        // Create right node and move pointers and records
        // +1 cause the first value gets moved up??

        let split_entry = self.decode_data_entry((self.header.items_stored as usize) - to_move - 1)?;

        {
            // TODO: Handle new offsets

            let free_space_start = NODE_HEADER_SIZE + (4*to_move);
    
            // Data entries, removes each entry from source node at same time
            let mut free_space_end = PAGE_SIZE;
            for i in (self.header.items_stored as usize)-to_move..self.header.items_stored as usize { 
                // free_space_end -= 8 + 
                let record = self.decode_data_entry(i)?;
                let new_free_space_end = free_space_end - Self::get_entry_size(&record);

                Node::encode_data_entry(&mut right_page_buffer[new_free_space_end..free_space_end], &record);
                free_space_end = new_free_space_end;

                self.remove_data_entry(i)?;
            }

            // Header
            let right_node_header = NodeHeader::get(
                self.header.node_type.clone(),
                (NODE_HEADER_SIZE + 4*to_move) as u32,
                free_space_end as u32,
                to_move as u32);

            bincode::encode_into_slice(
                &right_node_header,
                &mut right_page_buffer[..NODE_HEADER_SIZE],
                bincode::config::standard());

            // Offset array
            bincode::encode_into_slice(
                &self.offsets_array[(self.header.items_stored as usize)-to_move..],
                &mut right_page_buffer[NODE_HEADER_SIZE..NODE_HEADER_SIZE+(4*to_move)],
                bincode::config::standard());
        }

        // Create upper node and move the split pointer
        {
            let entry_size = Self::get_entry_size(&split_entry);
            // Header
            let top_node_header = NodeHeader::get(
                NodeType::Internal, // has to be internal cause moving up
                NODE_HEADER_SIZE as u32+4,
                (PAGE_SIZE - entry_size) as u32,
                1);

            bincode::encode_into_slice(
                &top_node_header,
                &mut top_page_buffer[..NODE_HEADER_SIZE],
                bincode::config::standard());

            // Offset array
            // bincode::encode

            // Split entry
            let offset = top_node_header.free_space_end as usize;
            Node::encode_data_entry(&mut top_page_buffer[offset..], &split_entry);
        }

        pager.save_page(top_page_buffer.to_vec(), None);
        pager.save_page(right_page_buffer.to_vec(), None);

        Ok(Node::deserialize(0, pager)?)
    }
}

