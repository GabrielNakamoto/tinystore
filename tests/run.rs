use rand::distr::{Alphanumeric, SampleString};
use std::collections::HashMap;
use log::info;
use anyhow::Result;
use tinystore::store::Connection;
use std::path::Path;


#[test]
fn fill_and_query() {
    env_logger::init();
    let n = 20000;
    let mut items: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();

    for i in 0..n {
        let key = Alphanumeric.sample_string(&mut rand::rng(), 10).into_bytes();
        let value = Alphanumeric.sample_string(&mut rand::rng(), 6).into_bytes();

        items.insert(key, value);
    }

    let path = Path::new("mydb");
    let mut connection = Connection::open(&path).unwrap();


    for (key, value) in &items {
        connection.put(&key, &value).unwrap();
    }

    let mut successful = 0;
    for (i, (key, value)) in items.iter().enumerate() {
        // info!("Item {i}");
        match connection.get(&key) {
            Ok(rvalue) => {
                assert_eq!(rvalue, *value);
                successful += 1;
            },
            Err(_) => {
                info!("Couldn't find {i}th test key");
            }
        }
    }
    info!("{} / {} successful", successful, n);
}
