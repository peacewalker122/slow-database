use clap::Parser;
use log::{debug, error, info, trace, warn};
use wasm_kv::api::api::KVEngine;
use wasm_kv::{Config, PersistentKV};

fn main() {
    // Parse command line arguments
    let config = Config::parse();

    // Initialize logger
    config.init_logger();

    info!("Starting wasm-kv key-value store");
    info!("Data directory: {}", config.data_dir);
    info!("Log level: {:?}", config.get_log_level());

    // Example usage demonstrating different log levels
    debug!("Initializing PersistentKV instance");
    let mut kv = PersistentKV::new();

    // Example operations that will trigger logging
    info!("Performing sample operations...");

    trace!("About to put key1");
    match kv.put(b"key1", b"value1") {
        Ok(_) => info!("Successfully stored key1"),
        Err(e) => error!("Failed to store key1: {}", e),
    }

    trace!("About to get key1");
    match kv.get(b"key1") {
        Ok(val) => {
            if !val.is_empty() {
                info!("Retrieved key1: {:?}", String::from_utf8_lossy(&val));
            } else {
                warn!("key1 not found");
            }
        }
        Err(e) => error!("Failed to get key1: {}", e),
    }

    trace!("About to delete key1");
    kv.delete(b"key1");
    info!("Deleted key1");

    info!("Sample operations completed");
}
