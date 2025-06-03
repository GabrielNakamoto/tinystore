use crate::{
    constants::{
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

    // TODO: ensure mutabiliy is correct,
    // for example since we arent updating the file in
    // this function it should be all immutable slices of the page
    let node_header = node.get_header()?;
    let rptrs = node.get_record_pointers()?;

    let page_buffer = &mut node.page_buffer;

    for ptr in rptrs {
        let offset = ptr as usize;

        let mut decoded : u32 = bincode::decode_from_slice(
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
    // TODO: When inserting a new record pointer put the ptr in order of key comparisons?

    // get root
    // TODO: handle bincode results
    let mut node = Node::find_node(&key, pager)?;
    let mut node_header = node.get_header()?;

    // Overflowed
    if node_header.free_space_end - node_header.free_space_start < (key.len() + value.len()) as u32 {
        let new_node = node.split(pager)?;
    }

    let mut page_buffer = &mut node.page_buffer;
    let header_start = if node.page_id == 0 { DB_HEADER_SIZE } else { 0 } as usize;

    // for each (key, value)
    // store in metadata:
    // -    offset to start of each record (key, value pair) -> 4 bytes
    //      and length of key and value

    let record_offset = node_header.free_space_end-((key.len() + value.len() + 8) as u32);
    let key_size = key.len() as u32;
    let value_size = value.len() as u32;

    // Update Metadata Section
    let meta_end_offset = node_header.free_space_start as usize;
    let meta_entry_slice = &mut page_buffer[meta_end_offset..meta_end_offset+4];
    bincode::encode_into_slice(record_offset, meta_entry_slice, bincode::config::standard());

    // TODO: Store key value sizes in record section? Only store offsets at start

    // Update Record Section
    let mut record_slice =
        &mut page_buffer[record_offset as usize..node_header.free_space_end as usize];

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
