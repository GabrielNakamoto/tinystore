use log::{ debug, info };
use std::path::Path;
use crate::pager::Pager;

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
            pager.save_page(&Pager::allocate_page_buffer(), Some(0));

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

    pub fn insert(key : &[u8], value : &[u8]) {

        // tell b-tree to insert and pass in pager??
    }
}

