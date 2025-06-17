use anyhow::Result;
use log::info;
use rand::distr::{Alphanumeric, SampleString};
use std::collections::HashMap;
use std::path::Path;
use std::time::{Duration, Instant};
use tinystore::store::Connection;

fn generate_entries(
    n_entries: usize,
    key_len: usize,
    value_len: usize,
) -> HashMap<Vec<u8>, Vec<u8>> {
    let mut items: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();

    for i in 0..n_entries {
        let key = loop {
            let x = Alphanumeric
                .sample_string(&mut rand::rng(), key_len)
                .into_bytes();
            if items.get(&x).is_none() && x != vec![0u8; key_len] {
                break x;
            }
        };
        let value = Alphanumeric
            .sample_string(&mut rand::rng(), value_len)
            .into_bytes();

        items.insert(key, value);
    }

    items
}

fn print_benchmark(
    insertion_elapsed: Duration,
    query_elapsed: Duration,
    n_entries: usize,
    key_len: usize,
    value_len: usize,
    successful: usize,
) {
    let total_elapsed = insertion_elapsed + query_elapsed;
    let bytes_stored = n_entries * (key_len + value_len);
    let is = insertion_elapsed.as_secs();
    let qs = query_elapsed.as_secs();
    let ims = insertion_elapsed.as_millis();
    let qms = query_elapsed.as_millis();
    let ts = total_elapsed.as_secs();
    let tms = total_elapsed.as_millis();

    let benchmark = format!(
        "
\nInserted {} {} byte key {} byte value records
\n{} / {} successful
------------------------------
Records lost:\t\t{}
Data bytes stored:\t{}mb\t{}kb
Insertion took:\t\t{}kb\\s\t{}s\t{}ms
Fetching took:\t\t{}kb\\s\t{}s\t{}ms
Overall:\t\t\t{}s\t{}ms\n",
        n_entries,
        key_len,
        value_len,
        successful,
        n_entries,
        n_entries - successful,
        bytes_stored / 1e6 as usize,
        bytes_stored / 1e3 as usize,
        bytes_stored as u128 / ims,
        is,
        ims,
        bytes_stored as u128 / qms,
        qs,
        qms,
        ts,
        tms
    );

    info!("{}", benchmark);
}

// TODO: make multiple tests
#[test]
fn fill_and_query() {
    env_logger::init();
    const n: usize = 120000;
    const kl: usize = 10;
    const vl: usize = 6;

    let items = generate_entries(n, kl, vl);

    let path = Path::new("mydb");
    let mut connection = Connection::open(&path).unwrap();

    let now = Instant::now();
    for (key, value) in &items {
        connection.put(&key, &value).unwrap();
    }
    let insertion_elapsed = now.elapsed();

    let mut successful = 0;
    for (i, (key, value)) in items.iter().enumerate() {
        match connection.get(&key) {
            Ok(rvalue) => {
                assert_eq!(rvalue, *value);
                successful += 1;
            }
            Err(_) => {
                // info!("Couldn't find {i}th test key");
            }
        }
    }
    let total_elapsed = now.elapsed();

    print_benchmark(
        insertion_elapsed,
        total_elapsed - insertion_elapsed,
        n,
        kl,
        vl,
        successful,
    );
}
