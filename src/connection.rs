use log::{ debug, info };
use std::path::Path;
use crate::pager::{Pager, constants};
use crate::btree;
use bincode::config;

pub struct Config {
}

pub struct Connection {
    pager : Pager
}

impl Connection {
    fn try_initialize_db(pager : &mut Pager) -> std::io::Result<()> {
        let mut page_buffer = Pager::allocate_page_buffer();

        let bytes_read = pager.get_page(page_buffer.as_mut_slice(), 0)?;

        if bytes_read == 0 {
            // initialize db header / root node
            let mut root_node = btree::operations::create_leaf_node();
            // TODO: better way of converting
            let mut payload = vec![0u8; (constants::PAGE_SIZE-constants::DB_HEADER_SIZE) as usize];
            bincode::encode_into_slice(root_node.header, payload.as_mut_slice(), config::standard());

            pager.save_page(&mut payload, Some(0));

            info!("Initializing database header and root node");
        } else {
            info!("Database already initialized");
        }

        Ok(())
    }

    pub fn open(db_path : &Path, config : Config) -> std::io::Result<Connection> {
        let mut pager = Pager::new(db_path)?;

        Self::try_initialize_db(&mut pager);

        info!("Opened new database connection");

        Ok(Connection {
            pager,
        })
    }

    pub fn insert(&mut self, key : Vec<u8>, value : Vec<u8>) {
        // tell b-tree to insert and pass in pager??
        btree::operations::find_node(key, &mut self.pager);
    }
}

