use log::info;
use std::path::Path;
use std::collections::HashMap;
use tinystore::connection::{Connection, Config};

use rand::{thread_rng, Rng};


fn random_string(n : usize) -> String {
    thread_rng()
        .sample_iter(rand::distr::Alphanumeric)
        .take(n)
        .map(char::from)
        .collect()
}

fn main() {
    env_logger::init();
    let n : u32 = 500;
    let mut entries : HashMap<String, String> = HashMap::new();

    // Generate random entries
    for _ in 0..n {
        entries.insert(random_string(10), random_string(5));
    }

    // Initialize connection
    let db_path = Path::new("db");
    let mut connection = Connection::open(&db_path, Config{})
        .expect("Failed to connect to DB");

    // Populate db
    info!("Populating test values");
    for (key, value) in &entries {
        connection.put(key.clone().into_bytes(), value.clone().into_bytes());
    }

    info!("Verifying test values");
    // Verify values
    for (index, (key, value)) in entries.iter().enumerate() {
        match connection.get(key.clone().into_bytes()) {
            Ok(returned_value) => {
                info!("Got entry {}", index);
                assert_eq!(value.clone().into_bytes(), returned_value);
            },
            Err(e) => {
                info!("Couldn't find entry {}: {}", index, e);
            }
        }
    }
}
