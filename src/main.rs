use std::path::Path;
use tinystore::connection::{Connection, Config};


fn main() {
    env_logger::init();
    let path = Path::new("db");

    let connection = Connection::open(path, Config{});
}
