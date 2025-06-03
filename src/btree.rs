mod node {
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


    #[derive(Decode, Encode)]
    pub struct RecordPtr {
        pub page_offset: u32,
        pub key_length: u32,
        pub value_length: u32
    }

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
        pub page_buffer : Vec<u8>
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

        // 
        // Insert keys in increasing order (maybe just sort the offset ptrs)
        // When there is no more room for a new key, move it to a new 
        // node to the right and move the current greatest key from source
        // node up to make a new one
        //
        pub fn split(&mut self, pager : &mut Pager) -> std::io::Result<Node> {
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


    // pub fn split_node(src_file_id : u32, page : &mut Pager) -> std::io::Result<u32> {
    //     Ok(0)
    // }
}

pub mod operations {
    use crate::{
        constants::{
            DB_HEADER_SIZE,
            PAGE_SIZE
        },
        pager::Pager
    };
    use super::node::{
        Node,
        RecordPtr,
        NodeType,
        NodeHeader,
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
        let mut node = Node::find_node(&key, pager)?;
        // let page_id = node::find_node(&key, pager)?;

        // TODO: ensure mutabiliy is correct,
        // for example since we arent updating the file in
        // this function it should be all immutable slices of the page
        // let (mut page_buffer, bytes_read) = pager.get_page(page_id)?;
        // let (mut node_header, bytes_decoded) : (NodeHeader, usize) = bincode::decode_from_slice(
        //     &page_buffer[header_start..],
        //     bincode::config::standard()).unwrap();
        let mut node_header = node.get_header()?;
        let mut page_buffer = &mut node.page_buffer;

        // iterate through each record
        let header_start = if node.page_id == 0 { DB_HEADER_SIZE } else { 0 } as usize;
        let records_start = header_start + NODE_HEADER_SIZE;
        let records_slice = &page_buffer[records_start..node_header.free_space_start as usize];

        // expect length to be a multiple of record pointer length (times # of items in header)
        for i in 0..node_header.items_stored {
            let rptr_start_offset = (i*12) as usize;

            let mut decoded : u32 = 0;
            decoded = bincode::decode_from_slice(
                &records_slice[rptr_start_offset..rptr_start_offset+4],
                bincode::config::standard()).unwrap().0;

            let offset = decoded as usize;

            decoded = bincode::decode_from_slice(
                &page_buffer[offset..offset+4],
                bincode::config::standard()).unwrap().0;

            let key_len = decoded as usize;

            let record_key = &page_buffer[offset+8..offset+8+key_len];

            if record_key == key.as_slice() {
                decoded = bincode::decode_from_slice(
                    &page_buffer[offset+4..offset+8],
                    bincode::config::standard()).unwrap().0;

                let value_len = decoded as usize;

                let value_start = (offset + 8 + key_len) as usize;
                let record_value = &page_buffer[value_start..value_start + value_len];

                return Ok(record_value.to_vec());
            }
        }

        Ok(String::from("Error Lmao").into_bytes())
    }

    pub fn insert_record(mut key : Vec<u8>, mut value : Vec<u8>, pager : &mut Pager) -> std::io::Result<()> {
        // get root
        let mut node = Node::find_node(&key, pager)?;
        let mut node_header = node.get_header()?;
        // TODO: handle bincode results
        // let (mut page_buffer, bytes_read) = pager.get_page(page_id)?;
        // let (mut node_header, bytes_decoded) : (NodeHeader, usize) = bincode::decode_from_slice(
        //     &page_buffer[header_start..],
        //     bincode::config::standard()).unwrap();

        // Overflowed
        if node_header.free_space_end - node_header.free_space_start < (key.len() + value.len()) as u32 {
            // New page id, could be the same page
            let new_node = node.split(pager)?;
        }

        let mut page_buffer = &mut node.page_buffer;
        let header_start = if node.page_id == 0 { DB_HEADER_SIZE } else { 0 } as usize;

        // for each (key, value)
        // store in metadata:
        // -    offset to start of each record (key, value pair) -> 4 bytes
        //      and length of key and value

        // TODO: check for overflow

        // These offsets are different for first page
        let rptr = RecordPtr {
            page_offset: node_header.free_space_end-((key.len() + value.len() + 8) as u32),
            key_length: key.len() as u32,
            value_length: value.len() as u32
        };

        // let record_offset = rptr.page_offset as usize;
        let record_offset = node_header.free_space_end-((key.len() + value.len() + 8) as u32);
        let key_size = key.len() as u32;
        let value_size = value.len() as u32;

        // Update Metadata Section
        let meta_end_offset = node_header.free_space_start as usize;
        let mut meta_entry_slice = &mut page_buffer[meta_end_offset..meta_end_offset+4];
        bincode::encode_into_slice(record_offset, meta_entry_slice, bincode::config::standard());

        // TODO: Store key value sizes in record section? Only store offsets at start

        // Update Record Section
        let mut record_slice = &mut page_buffer[record_offset as usize..node_header.free_space_end as usize];

        bincode::encode_into_slice(
            key.len() as u32, &mut record_slice[..4],
            bincode::config::standard());
        bincode::encode_into_slice(
            value.len() as u32, &mut record_slice[4..8],
            bincode::config::standard());
        &mut record_slice[8..8 as usize+key.len()]
            .copy_from_slice(key.as_mut_slice());
        &mut record_slice[8+key.len() as usize..]
            .copy_from_slice(value.as_mut_slice());

        // Update node header
        node_header.free_space_start += 4;
        node_header.free_space_end -= (key.len() + value.len() + 8) as u32;
        node_header.items_stored += 1;

        let mut header_slice = &mut page_buffer[header_start..header_start+NODE_HEADER_SIZE as usize];

        bincode::encode_into_slice(node_header, header_slice, bincode::config::standard());

        // Request changes to page cache
        //      Look at buffered writes
        pager.save_page(page_buffer.to_vec(), Some(node.page_id));

        // decode child pointers, if this node is a leaf then I can just insert
        // println!("Root header: {:#?}", root_header);

        // search for node / page containing this key
        // update with record metada and values
        Ok(())
    }
}

