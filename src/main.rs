use std::path::Path;
use tinystore::connection::{Connection, Config};


fn main() {
    env_logger::init();
    let path = Path::new("db");

    let mut connection = Connection::open(path, Config{}).unwrap();
    let key : Vec<u8> = String::from("Gabriel").into_bytes();
    connection.insert(key, vec![0u8; 4]);
}
