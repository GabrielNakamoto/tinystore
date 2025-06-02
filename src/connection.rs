use std::path::Path;
use crate::{
    constants::{
        DBHeader,
        DB_HEADER_SIZE
    },
    pager::Pager,
    btree
};

pub struct Config {
}

pub struct Connection {
    pager : Pager
}

///
/// Public user / application interface 
///
/// Put(key, value)
/// Get(key) -> value
/// Delete(key)
///
///     *Put will update value if key already exists
///
impl Connection {
    pub fn open(db_path : &Path, config : Config) -> std::io::Result<Connection> {
        let mut connection = Connection {
            pager: Pager::new(db_path)?,
        };

        connection.try_initialize_db(db_path);

        Ok(connection)
    }

    // 
    // Create new database file if necessary 
    // Add empty root node along with db metadata
    // on first page.
    //
    fn try_initialize_db(&mut self, db_path : &Path) -> std::io::Result<()> {
        let (first_page, bytes_read) = self.pager.get_page(0)?;

        if bytes_read == 0 {
            let mut payload = Pager::allocate_page_buffer();
            let mut header_slice = &mut payload[..DB_HEADER_SIZE];
            // TODO: check for error
            bincode::encode_into_slice(DBHeader::get(), header_slice, bincode::config::standard());

            btree::operations::initialize_tree(&mut payload);

            self.pager.save_page(payload, None)?;
        }

        Ok(())
    }

    pub fn put(&mut self, key : Vec<u8>, value : Vec<u8>) -> std::io::Result<()> {
        btree::operations::insert_record(key, value, &mut self.pager)?;
        Ok(())
    }
    pub fn get(&mut self, key : Vec<u8>) -> std::io::Result<Vec<u8>> {
        btree::operations::get_record(key, &mut self.pager)
    }
    // pub fn delete(key : Vec<u8>) -> std::io::Result<()> {
    // }
}
