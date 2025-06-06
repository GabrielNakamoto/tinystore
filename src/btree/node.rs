use prev_iter::PrevPeekable;
use log::{info, warn, debug};
use super::entry::DataEntry;
use crate::{
    constants::{
        DB_HEADER_SIZE,
        PAGE_SIZE
    },
    pager::Pager
};
use bincode::{Decode, Encode};

pub const NODE_HEADER_SIZE : usize = 30;
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
    pub first_free_block_offset : u16,
    pub rightmost_child: u32,
    pub parent: i32
}

impl NodeHeader {
    pub fn new(node_type: NodeType) -> Self {
        Self {
            magic_numbers: [80, 65, 71, 69],
            node_type,
            free_space_start: NODE_HEADER_SIZE as u32,
            free_space_end: PAGE_SIZE as u32,
            items_stored: 0,
            first_free_block_offset: 0,
            rightmost_child: 0,
            parent: -1
        }
    }

    pub fn get(node_type : NodeType, free_space_start : u32, free_space_end : u32, items_stored : u32) -> Self {
        Self {
            magic_numbers: [80, 65, 71, 69],
            node_type,
            free_space_start,
            free_space_end,
            items_stored,
            first_free_block_offset: 0,
            rightmost_child: 0,
            parent: -1
        }
    }
}

pub struct Node {
    pub page_id : u32,
    pub page_buffer : Vec<u8>,
    pub header: NodeHeader,
    pub offsets_array : Vec<u32>
}

#[derive(Decode, Encode, Clone, Copy, Debug)]
pub struct FreeBlock {
    pub next_ptr: u16,
    pub total_size: u16
}

pub struct FreeBlockIter<'a> {
    pub parent: &'a Node,
    pub curr: Option<FreeBlock>
}

impl<'a> Iterator for FreeBlockIter<'a> {
    type Item = FreeBlock;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(block) = self.curr {
            if block.next_ptr == 0 {
                self.curr = None;
                return Some(block);
            }

            let start = block.next_ptr as usize;
            let decoded = bincode::decode_from_slice(
                &self.parent.page_buffer[start..start+4],
                bincode::config::standard()).ok()?.0;

            self.curr = Some(decoded);

            Some(block)
        } else {
            None
        }
    }
}

impl<'a> FreeBlockIter<'a> {
    pub fn new(parent : &'a Node) -> Option<Self> {
        let block = parent.first_free_block()?;

        Some(Self {
            parent,
            curr: Some(block)
        })
    }
}

impl Node {
    pub fn first_free_block(&self) -> Option<FreeBlock> {
        if self.header.first_free_block_offset == 0 {
            debug!("No free blocks");
            None
        } else {
            let start = self.header.first_free_block_offset as usize;
            let end = start + 4;

            let block : FreeBlock = bincode::decode_from_slice(
                &self.page_buffer[start..end], bincode::config::standard()).ok().map(|(block, _)| block)?;

            debug!("Root free block at: {}", start);
            Some(block)
        }
    }

    pub fn new(node_type : NodeType, pager : &mut Pager) -> Self {
        Self {
            page_id: pager.next_page_id(),
            page_buffer: Pager::allocate_page_buffer(),
            header: NodeHeader::new(node_type),
            offsets_array: Vec::new()
        }
    }

    pub fn deserialize(page_id : u32, pager : &mut Pager) -> std::io::Result<Self> {
        let (page_buffer, bytes_read) = pager.get_page(page_id)?;

        let header = Self::get_header(page_id, &page_buffer)?;
        let offsets_array = Self::get_offsets_array(&header, page_id, &page_buffer)?;

        assert_eq!(header.items_stored as usize, offsets_array.len());

        Ok(Node {
            page_id,
            page_buffer,
            header,
            offsets_array
        })
    }

    // TODO: error handling
    pub fn encode_header(&mut self)  {
        let header_start = if self.page_id == 0 { DB_HEADER_SIZE } else { 0 } as usize;

        bincode::encode_into_slice(
            &self.header,
            &mut self.page_buffer[header_start..header_start+NODE_HEADER_SIZE],
            bincode::config::standard());
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
        let entry_offset = self.offsets_array.get(entry_id).copied().unwrap() as usize;

        DataEntry::decode(&self.page_buffer, entry_offset, &self.header.node_type)
    }

    fn encode_entry_offset(&mut self, new_entry : &DataEntry, entry_offset : u32) {
        // Find spot where entry belongs
        let mut right_index = self.header.items_stored + 1;
        for i in 0..self.header.items_stored {
            let entry = self.decode_data_entry(i as usize).unwrap();

            if entry.key() > new_entry.key() {
                right_index = i;
                break;
            }
        }

        debug!("Offset right index: {}", right_index);
        debug!("New entry offset: {}", entry_offset);
        let mut entry_slice = if right_index == self.header.items_stored + 1 {
            debug!("Appending offset to end of array");
            // Add to end
            let start = self.header.free_space_start as usize;
            
            &mut self.page_buffer[start..start+4]
        } else {
            debug!("Adding offset into middle of array");

            // Shift everything to right over
            let to_shift = self.header.items_stored - right_index;
            let end = self.header.free_space_start as usize;
            let start = end - (4*to_shift as usize);
            self.page_buffer.as_mut_slice().copy_within(start..end, start+4);

            &mut self.page_buffer[start..start+4]
        };

        bincode::encode_into_slice(&entry_offset, entry_slice, bincode::config::standard());

        self.header.free_space_start += 4;
    }
    
    pub fn insert_data_entry(&mut self, new_entry : &DataEntry) {
        // Search free blocks first
        // Handle none
        if let Some(mut iter) = FreeBlockIter::new(self) {
            let available_index = iter.position(|block| block.total_size as u32 >= new_entry.size());
            if let Some(idx) = available_index {
                info!("Found available free block");
                // TODO: make function that will remove a free block and update ptrs

                let new_offset = if idx - 1 < 0 {
                    self.header.first_free_block_offset
                } else {
                    // get offset from previous block
                    iter.nth(idx - 1).unwrap().next_ptr
                } as usize;

                let entry_slice =
                    &mut self.page_buffer[new_offset..new_offset+new_entry.size() as usize];
                new_entry.encode(entry_slice);

                self.encode_entry_offset(&new_entry, new_offset as u32);

                self.header.items_stored += 1;
                self.encode_header();
                return;
            };
        }

        debug!("Appending entry to free space");

        // No available free blocks that fit requirements
        if self.header.free_space_end - self.header.free_space_start < new_entry.size() as u32 {
            // TODO: handle overflow!!, make overflow error type
        }

        let record_offset = (self.header.free_space_end - new_entry.size()) as usize;
        new_entry.encode(&mut self.page_buffer[record_offset..self.header.free_space_end as usize]);
        self.encode_entry_offset(&new_entry, record_offset as u32);

        self.header.items_stored += 1;
        self.header.free_space_end -= new_entry.size() as u32;
        self.encode_header();
        
        self.offsets_array = Self::get_offsets_array(&self.header, self.page_id, &self.page_buffer).unwrap();
        // Double check changes worked?

        // let offsets = Self::get_offsets_array(&self.header, self.page_id, &self.page_buffer).unwrap();
        // debug!("Offsets: {:#?}", offsets);
    }

    // TODO: function to insert new entry, finds best empty spot for it and encodes it

    // Optimize this, to batch together removals / updating offsets array
    pub fn remove_data_entry(&mut self, entry_id : usize) -> std::io::Result<()> {
        let entry_offset = self.offsets_array[entry_id] as usize;

        let iter = FreeBlockIter::new(self);

        // Update block chain / page header
        let next_ptr = match iter {
            Some(mut iter) => {
                // TODO: handle option => results
                // let mut prev_iter = PrevPeekable::new(iter);
                let mut prev_vec : Vec<_> = iter.collect();

                let update_block_index = prev_vec.iter()
                    .position(|block| {
                        block.next_ptr > entry_offset as u16
                    }).unwrap_or(prev_vec.len() - 1);

                let mut update_block = prev_vec[update_block_index].clone();

                let left_ptr = if update_block_index > 0 {
                    prev_vec[update_block_index-1].next_ptr
                } else {
                    self.header.first_free_block_offset
                } as usize;

                let info = update_block;
                let right_ptr = update_block.next_ptr as usize;
                update_block.next_ptr = entry_offset as u16;
                debug!("Updated left free block:\n{:?} => {:?}", info, update_block);

                // Update previous node in linked list to contain ptr to new block
                bincode::encode_into_slice(
                    &update_block,
                    &mut self.page_buffer[left_ptr..left_ptr+4],
                    bincode::config::standard());

                // Return ptr to next block
                right_ptr
            },
            None => {
                // Add to page header
                self.header.first_free_block_offset = entry_offset as u16;
                self.encode_header();

                0
            }
        };

        // Create free block
        let entry = self.decode_data_entry(entry_id)?;
        let block = FreeBlock {
            next_ptr: next_ptr as u16,
            total_size: entry.size() as u16
        };
    
        debug!("New free block: {:#?}", block);

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
        self.encode_header();
        self.offsets_array = Self::get_offsets_array(&self.header, self.page_id, &self.page_buffer).unwrap();
        Ok(())
    }
}
