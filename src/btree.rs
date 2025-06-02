mod node {
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
        RecordPtr,
        NodeType,
        NodeHeader,
        NODE_HEADER_SIZE
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

    // make find node function, returns the page id of the leaf node where
    // the key belongs
    fn find_node(key : &Vec<u8>, pager : &mut Pager) -> std::io::Result<u32> {
        let mut cur_page_id = 0; // start at root 

        while true {
            let (mut page_buffer, bytes_read) = pager.get_page(0)?;
            let (mut node_header, bytes_decoded) : (NodeHeader, usize) = bincode::decode_from_slice(
                &page_buffer[DB_HEADER_SIZE..],
                bincode::config::standard()).unwrap();

            if node_header.node_type == NodeType::Leaf {
                return Ok(cur_page_id)
            }
        }

        // TODO: make this an Err
        Ok(0)
    }

    pub fn get_record(mut key : Vec<u8>, pager : &mut Pager) -> std::io::Result<Vec<u8>> {
        let page_id = find_node(&key, pager)?;

        // TODO: ensure mutabiliy is correct,
        // for example since we arent updating the file in
        // this function it should be all immutable slices of the page
        let (mut page_buffer, bytes_read) = pager.get_page(page_id)?;
        let header_start = if page_id == 0 { DB_HEADER_SIZE } else { 0 } as usize;
        let (mut node_header, bytes_decoded) : (NodeHeader, usize) = bincode::decode_from_slice(
            &page_buffer[header_start..],
            bincode::config::standard()).unwrap();

        // iterate through each record
        let records_start = header_start + NODE_HEADER_SIZE;
        let records_slice = &page_buffer[records_start..node_header.free_space_start as usize];

        // expect length to be a multiple of record pointer length (times # of items in header)
        for i in 0..node_header.items_stored {
            let rptr_start_offset = (i*12) as usize;
            let (rptr, bytes_decoded) : (RecordPtr, usize) = bincode::decode_from_slice(
                &records_slice[rptr_start_offset..rptr_start_offset+12 as usize],
                bincode::config::standard())
                .unwrap();

            // go to record pointer offset
            let record_key = &page_buffer[rptr.page_offset as usize..(rptr.page_offset+rptr.key_length) as usize];
            let record_value = &page_buffer[(rptr.page_offset+rptr.key_length) as usize..(rptr.page_offset+rptr.key_length+rptr.value_length) as usize];

            if record_key == key.as_slice() {
                return Ok(record_value.to_vec());
            }
        }

        Ok(String::from("Error Lmao").into_bytes())
    }

    pub fn insert_record(mut key : Vec<u8>, mut value : Vec<u8>, pager : &mut Pager) -> std::io::Result<()> {
        // get root

        let page_id = find_node(&key, pager)?;

        // TODO: handle bincode results
        let (mut page_buffer, bytes_read) = pager.get_page(page_id)?;
        let header_start = if page_id == 0 { DB_HEADER_SIZE } else { 0 } as usize;
        let (mut node_header, bytes_decoded) : (NodeHeader, usize) = bincode::decode_from_slice(
            &page_buffer[header_start..],
            bincode::config::standard()).unwrap();

        // for each (key, value)
        // store in metadata:
        // -    offset to start of each record (key, value pair) -> 4 bytes
        //      and length of key and value

        // TODO: check for overflow

        // These offsets are different for first page
        let rptr = RecordPtr {
            page_offset: node_header.free_space_end-((key.len() + value.len()) as u32),
            key_length: key.len() as u32,
            value_length: value.len() as u32
        };

        let record_offset = rptr.page_offset as usize;

        // Update Metadata Section
        let meta_end_offset = node_header.free_space_start as usize;
        let mut meta_entry_slice = &mut page_buffer[meta_end_offset..meta_end_offset+12];

        bincode::encode_into_slice(rptr, meta_entry_slice, bincode::config::standard());

        // Update Record Section
        let mut record_slice = &mut page_buffer[record_offset..node_header.free_space_end as usize];
        &mut record_slice[..key.len() as usize].copy_from_slice(key.as_mut_slice());
        &mut record_slice[key.len() as usize..(key.len() + value.len()) as usize]
            .copy_from_slice(value.as_mut_slice());

        // Update node header
        node_header.free_space_start += 12;
        node_header.free_space_end -= (key.len() + value.len()) as u32;
        node_header.items_stored += 1;

        let mut header_slice = &mut page_buffer[header_start..header_start+NODE_HEADER_SIZE as usize];

        bincode::encode_into_slice(node_header, header_slice, bincode::config::standard());

        // Request changes to page cache
        //      Look at buffered writes
        pager.save_page(page_buffer, Some(page_id));

        // decode child pointers, if this node is a leaf then I can just insert
        // println!("Root header: {:#?}", root_header);

        // search for node / page containing this key
        // update with record metada and values
        Ok(())
    }
}

