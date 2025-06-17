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

fn insert_items(connection: &mut Connection, items: &HashMap<Vec<u8>, Vec<u8>>) -> Duration {
    let now = Instant::now();
    for (key, value) in items {
        connection.put(key, value).unwrap();
    }

    now.elapsed()
}

fn get_items(connection: &mut Connection, items: &HashMap<Vec<u8>, Vec<u8>>) -> (usize, Duration) {
    let now = Instant::now();
    let mut successful: usize = 0;
    for (i, (key, value)) in items.iter().enumerate() {
        if let Ok(rvalue) = connection.get(&key) {
            assert_eq!(rvalue, *value);
            successful += 1;
        }
    }

    (successful, now.elapsed())
}

// TODO: make multiple tests
// TODO: understand iterators? Sequential insert / get
#[test]
fn fill_and_query() {
    env_logger::try_init();
    const n: usize = 10000;
    const kl: usize = 10;
    const vl: usize = 6;

    let items = generate_entries(n, kl, vl);

    let path = Path::new("test1");
    let mut connection = Connection::open(&path).unwrap();

    let insertion_elapsed = insert_items(&mut connection, &items);
    let (successful, query_elapsed) = get_items(&mut connection, &items);

    std::fs::remove_file("test1");

    print_benchmark(
        insertion_elapsed,
        query_elapsed,
        n,
        kl,
        vl,
        successful,
    );
}

#[test]
fn multiple_open_and_fill() {
    env_logger::try_init();
    const times: usize = 8;
    const n: usize = 50000;
    const kl: usize = 10;
    const vl: usize = 6;

    let mut total_lost = 0;
    let mut total_time: Duration = Duration::new(0, 0);

    for i in 0..times {
        let items = generate_entries(n, kl, vl);
        let path = Path::new("test2");
        let mut connection = Connection::open(&path).unwrap();
        let insertion_elapsed = insert_items(&mut connection, &items);
        let (successful, query_elapsed) = get_items(&mut connection, &items);

        total_lost += n - successful;
        total_time += insertion_elapsed + query_elapsed;

        print_benchmark(
            insertion_elapsed,
            query_elapsed,
            n,
            kl,
            vl,
            successful,
        );
    }

    std::fs::remove_file("test2");

    info!("Total lost: {}", total_lost);
    info!("Took:\t{}s\t{}ms", total_time.as_secs(), total_time.as_millis());
}
