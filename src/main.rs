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
    let n : u32 = 10000;
    let mut entries : HashMap<String, String> = HashMap::new();

    // Generate random entries
    let mut rng = rand::rng();
    for _ in 0..n {
        // let key_len = rng.random_range(1..500) as usize;
        // let value_len = rng.random_range(1..500) as usize;
        let key_len: usize = 10;
        let value_len: usize = 5;
        entries.insert(random_string(key_len), random_string(value_len));
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
    let mut successful = 0;
    for (index, (key, value)) in entries.iter().enumerate() {
        // let returned_value = connection.get(key.clone().into_bytes()).unwrap();
        // assert_eq!(value.clone().into_bytes(), returned_value);
        // successful += 1;
        match connection.get(key.clone().into_bytes()) {
            Ok(returned_value) => {
                successful += 1;
                assert_eq!(value.clone().into_bytes(), returned_value);
            },
            Err(e) => {
                // info!("Error getting value: {}", e);
            }
        }
    }
    info!("{} / {} entries recovered", successful, n);
}
