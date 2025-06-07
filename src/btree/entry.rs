use log::{info,debug};
use super::{
    node::NodeType,
    error::NodeError
};

#[derive(Debug)]
pub enum DataEntry {
    Internal(Vec<u8>, u32), // Key, Child ptr (page_id)
    Leaf(Vec<u8>, Vec<u8>), // Key, Value
}

impl DataEntry {
    pub fn size(&self) -> u32 {
        match self {
            Self::Leaf(key, value) => (8 + key.len() + value.len()) as u32,
            Self::Internal(key, page_id) => (8 + key.len()) as u32
        }
    }

    pub fn key(&self) -> &Vec<u8> {
        match self {
            Self::Leaf(key, _) => key,
            Self::Internal(key, _) => key
        }
    }

    pub fn decode(page_buffer : &Vec<u8>, entry_offset: usize, node_type : &NodeType) -> std::io::Result<Self> {
        // TODO: handle out of range error
    
        debug!("Decoding {:?} type data entry at offset: {}", node_type, entry_offset);
        let key_len : u32 = bincode::decode_from_slice(
            &page_buffer[entry_offset..entry_offset+4],
            bincode::config::standard()).unwrap().0;
        debug!("Entry key length: {}", key_len);
        match node_type {
            NodeType::Internal => {
                let page_id : u32 = bincode::decode_from_slice(
                    &page_buffer[entry_offset+4..entry_offset+8],
                    bincode::config::standard()).unwrap().0;
                let record_key = &page_buffer[entry_offset+8..entry_offset+8+(key_len as usize)];

                Ok(DataEntry::Internal(record_key.to_vec(), page_id))
            },
            NodeType::Leaf => {
                let value_len : u32 = bincode::decode_from_slice(
                    &page_buffer[entry_offset+4..entry_offset+8],
                    bincode::config::standard()).unwrap().0;

                // debug!("Value length: {}", value_len);
                let value_start = entry_offset+8+(key_len as usize);
                let record_key = &page_buffer[entry_offset+8..value_start];
                let record_value = &page_buffer[value_start..value_start+(value_len as usize)];

                // TODO: Make these slices not vecs?
                Ok(DataEntry::Leaf(record_key.to_vec(), record_value.to_vec()))
            }
        }
    }

    pub fn encode(&self, entry_slice : &mut [u8]) {
        match self {
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
}

