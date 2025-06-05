use prev_iter::PrevPeekable;
use super::entry::DataEntry;
use crate::{
    constants::{
        DB_HEADER_SIZE,
        PAGE_SIZE
    },
    pager::Pager
};
use bincode::{Decode, Encode};

pub const NODE_HEADER_SIZE : usize = 26;
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
    pub rightmost_child: u32
}

impl NodeHeader {
    pub fn get(node_type : NodeType, free_space_start : u32, free_space_end : u32, items_stored : u32) -> Self {
        Self {
            magic_numbers: [80, 65, 71, 69],
            node_type,
            free_space_start,
            free_space_end,
            items_stored,
            first_free_block_offset: 0,
            rightmost_child: 0
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
        let entry_offset = self.offsets_array.get(entry_id).copied().unwrap() as usize;

        DataEntry::decode(&self.page_buffer, entry_offset, &self.header.node_type)
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
                let mut prev_iter = PrevPeekable::new(iter);
                let mut block_before = prev_iter.find(|block| block.next_ptr > entry_offset as u16).unwrap();

                let ptr = block_before.next_ptr;
                block_before.next_ptr = entry_offset as u16;

                let before_ptr = match prev_iter.prev_peek() {
                    Some(block) => block.next_ptr,
                    None => self.header.first_free_block_offset
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
                // let header_start = if self.page_id == 0 { DB_HEADER_SIZE } else { 0 } as usize;
                // bincode::encode_into_slice(
                //     &self.header,
                //     &mut self.page_buffer[header_start..header_start+NODE_HEADER_SIZE],
                //     bincode::config::standard());
                self.encode_header();

                0
            }
        };

        // Create free block
        let entry = self.decode_data_entry(entry_id)?;
        let block = FreeBlock {
            next_ptr,
            total_size: entry.size() as u16
            // total_size: self.decode_data_entry(entry_id)?.size() as u16
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
}
