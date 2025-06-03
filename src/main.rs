use std::path::Path;
use tinystore::connection::{Connection, Config};


fn main() {
    let keys : Vec<&str> = vec!["Gabriel", "Kai", "Josh", "HDog123"];
    let values : Vec<&str> = vec!["95", "78", "83", "85"];
    let db_path = Path::new("db");
    let mut connection = Connection::open(&db_path, Config{})
        .expect("Failed to connect to DB");

    // for i in 0..keys.len() {
    //     connection.put(keys[i].as_bytes().to_vec(), values[i].as_bytes().to_vec());
    // }

    let returned_value = connection.get(String::from("HDog123").into_bytes()).unwrap();
    println!("{}", String::from_utf8(returned_value).unwrap());
}
