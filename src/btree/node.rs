use prev_iter::PrevPeekable;
use log::{info, warn, debug};
use super::{
    entry::DataEntry,
    error::NodeError
};
use crate::{
    constants::{
        DB_HEADER_SIZE,
        PAGE_SIZE
    },
    pager::Pager
};
use bincode::{Decode, Encode};

pub const NODE_HEADER_SIZE: usize = 30;
pub const M: usize = 5;
pub const PAGE_PTR_SIZE: usize = 2;

type PagePtr = u16;

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
    pub fn new(node_type: NodeType, free_space_start: u32) -> Self {
        Self {
            magic_numbers: [80, 65, 71, 69],
            node_type,
            free_space_start,
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

pub enum InsertResult {
    Success,
    NeedsSplit
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

    fn offsets_ptr(page_id: u32) -> PagePtr {
        (if page_id == 0 { DB_HEADER_SIZE + NODE_HEADER_SIZE } else { NODE_HEADER_SIZE }) as PagePtr
    }

    pub fn new(node_type : NodeType, pager : &mut Pager) -> Self {
        let page_id = pager.next_page_id();
        Self {
            page_id: page_id,
            page_buffer: Pager::allocate_page_buffer(),
            header: NodeHeader::new(node_type, Self::offsets_ptr(page_id) as u32),
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
        // debug!("Decode entry id: {}, 
        // debug!("Decoding entry: {} out of {}", entry_id, self.offsets_array.len());
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

        debug!("Right entry id: {}", right_index);
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
            debug!("Shifting {} items from {} => {}", to_shift, end, end+4);
            self.page_buffer.as_mut_slice().copy_within(start..end, start+4);

            &mut self.page_buffer[start..start+4]
        };

        bincode::encode_into_slice(&entry_offset, entry_slice, bincode::config::standard());

        self.header.free_space_start += 4;
    }

    fn find_block_offset(&self, vec : &Vec<FreeBlock>, index: usize) -> PagePtr {
        if index == 0 {
            self.header.first_free_block_offset as PagePtr
        } else {
            vec[index-1].next_ptr
        }
    }

    fn remove_free_block(&mut self, vec : &Vec<FreeBlock>, index: usize, next_ptr: PagePtr) -> Result<(), NodeError> {
        if index == 0 {
            self.header.first_free_block_offset = next_ptr;
        } else {
            let mut left_block = vec[index-1].clone();
            left_block.next_ptr = next_ptr;

            let left_offset = self.find_block_offset(vec, index-1) as usize;
            bincode::encode_into_slice(
                &left_block, 
                &mut self.page_buffer[left_offset..left_offset+4],
                bincode::config::standard())?;
        }
        Ok(())
    }

    // Updates block at index to point at new block offset, returning the offset
    // to the old 'right block'
    fn update_left_block(&mut self, blocklist: &Vec<FreeBlock>, index: usize, new_offset: PagePtr) -> PagePtr {
        let left_ptr = self.find_block_offset(&blocklist, index) as usize;

        let mut left_block = blocklist[index];
        let right_ptr = left_block.next_ptr;
        left_block.next_ptr = new_offset;

        bincode::encode_into_slice(
            &left_block,
            &mut self.page_buffer[left_ptr..left_ptr+4],
            bincode::config::standard());

        right_ptr
    }

    fn insert_free_block(&mut self, new_offset: PagePtr, block_size: u16) {
        let next_ptr = if let Some(iter) = FreeBlockIter::new(self) {
            let blocklist: Vec<FreeBlock> = iter.collect();
            let index = blocklist.iter()
                .position(|block| block.next_ptr > new_offset)
                .unwrap_or(blocklist.len()-1);

            self.update_left_block(&blocklist, index, new_offset)
        } else {
            self.header.first_free_block_offset = new_offset as PagePtr;
            0
        } as u16;

        let block = FreeBlock {
            next_ptr,
            total_size: block_size
        };

        let start = new_offset as usize;
        bincode::encode_into_slice(
            &block,
            &mut self.page_buffer[start..start+4],
            bincode::config::standard());
    }
    
    // Returns offset to start of free space
    fn find_and_remove_free_block(&mut self, min_capacity: u32) -> Option<PagePtr> {
        let iter_vec : Vec<_> = FreeBlockIter::new(self)?.collect();
        let idx = iter_vec.iter()
            .position(|block| block.total_size as u32 >= min_capacity)?;

        debug!("Found available free block at linked list index: {}", idx);

        let block = iter_vec[idx];

        let offset = self.find_block_offset(&iter_vec, idx);
        self.remove_free_block(&iter_vec, idx, block.next_ptr);

        Some(offset)
    }
    
    pub fn insert_data_entry(&mut self, new_entry : &DataEntry) -> InsertResult {
        info!("Inserting entry #{} at page id {}", self.header.items_stored + 1, self.page_id);
        let is_room : bool = self.header.free_space_end - self.header.free_space_start >= (new_entry.size() as u32 + 4);

        let offset = self.find_and_remove_free_block(new_entry.size() as u32)
            .unwrap_or_else(|| {
                debug!("Appending entry to free space");
                self.header.free_space_end -= new_entry.size() as u32;

                self.header.free_space_end as PagePtr
            }) as usize;

        if (! is_room) && offset as u32 == self.header.free_space_end {
            debug!("Node id: {} overflowed", self.page_id);
            return InsertResult::NeedsSplit;
        }

        new_entry.encode(&mut self.page_buffer[offset..offset+(new_entry.size() as usize)]);
        self.encode_entry_offset(&new_entry, offset as u32);

        self.header.items_stored += 1;
        self.encode_header();

        self.offsets_array = Self::get_offsets_array(&self.header, self.page_id, &self.page_buffer).unwrap();
        // for i in 0..self.header.items_stored {
        //     debug!("Offset {}: {}", i, self.offsets_array[i as usize]);
        // }

        InsertResult::Success
    }

    // Optimize this, to batch together removals / updating offsets array
    pub fn remove_data_entry(&mut self, entry_id : usize) -> std::io::Result<()> {
        info!("Removing entry #{} at page id {}", entry_id, self.page_id);
        let entry_offset = self.offsets_array[entry_id] as PagePtr;
        let entry = self.decode_data_entry(entry_id)?; // Error here?

        self.insert_free_block(entry_offset, entry.size() as u16);
    
        let slice_start = Self::offsets_ptr(self.page_id) as usize;
        let slice_end = self.header.free_space_start as usize;
        let mut offsets_slice = &mut self.page_buffer[slice_start..slice_end];

        let start = entry_id*4;
        // let end = self.header.free_space_start as usize;
        offsets_slice.copy_within(start.., start-4);

        self.header.free_space_start -= 4;
        self.header.items_stored -= 1;
        self.encode_header();

        self.offsets_array =
            Self::get_offsets_array(&self.header, self.page_id, &self.page_buffer).unwrap();

        if let Some(iter) = FreeBlockIter::new(self) {
            let blocklist: Vec<FreeBlock> = iter.collect();
            
            info!("Updated free list: {:#?}", blocklist);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use rand::{thread_rng, Rng};
    use rand::prelude::*;
    use crate::pager::Pager;
    use super::*;
    use std::path::Path;
    fn random_string(n : usize) -> String {
        thread_rng()
            .sample_iter(rand::distr::Alphanumeric)
            .take(n)
            .map(char::from)
            .collect()
    }

    #[test]
    fn insertion_test() -> std::io::Result<()> {
        let mut rng = rand::rng();

        let key_len = rng.random_range(1..=100) as usize;
        let value_len = rng.random_range(1..=100) as usize;
        let key = random_string(key_len).into_bytes();
        let value = random_string(value_len).into_bytes();

        let test_db = Path::new("testing_db");
        let mut pager = Pager::new(&test_db)?;
        let mut node = Node::new(NodeType::Leaf, &mut pager);

        let entry = DataEntry::Leaf(key.clone(), value.clone());

        node.insert_data_entry(&entry);
        if let DataEntry::Leaf(K, V) = node.decode_data_entry(0)? {
            assert_eq!(K, key);
            assert_eq!(V, value);
        }

        Ok(())
    }
}
