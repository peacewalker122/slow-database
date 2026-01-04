use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};

/// Manifest action types
const ACTION_ADD: u8 = 1;
const ACTION_REMOVE: u8 = 2;

/// Manifest file name
const MANIFEST_FILE: &str = "MANIFEST";

/// Manifest entry representing a file operation
#[derive(Debug, Clone, PartialEq)]
pub struct ManifestEntry {
    pub action: u8,
    pub level: u32,
    pub filename: String,
}

/// Encode a manifest entry to binary format
/// Format: <action: u8><level: u32><filename_len: u64><filename: [u8]><checksum: u32>
pub fn encode_manifest_entry(action: u8, level: u32, filename: &str) -> Vec<u8> {
    let filename_bytes = filename.as_bytes();
    let mut buf = Vec::with_capacity(1 + 4 + 8 + filename_bytes.len() + 4);

    // Build data for checksum calculation (everything except the checksum itself)
    let mut data_for_checksum = Vec::new();
    data_for_checksum.push(action);
    data_for_checksum.extend_from_slice(&level.to_be_bytes());
    data_for_checksum.extend_from_slice(&(filename_bytes.len() as u64).to_be_bytes());
    data_for_checksum.extend_from_slice(filename_bytes);

    // Calculate checksum
    let checksum = crc32fast::hash(&data_for_checksum);

    // Build final buffer
    buf.push(action);
    buf.extend_from_slice(&level.to_be_bytes());
    buf.extend_from_slice(&(filename_bytes.len() as u64).to_be_bytes());
    buf.extend_from_slice(filename_bytes);
    buf.extend_from_slice(&checksum.to_be_bytes());

    buf
}

/// Decode a manifest entry from a reader at a specific offset
pub fn decode_manifest_entry<R>(
    mut reader: R,
    offset: u64,
) -> Result<(ManifestEntry, u64), std::io::Error>
where
    R: Read + Seek,
{
    reader.seek(SeekFrom::Start(offset))?;

    // Read action
    let mut action_buf = [0u8; 1];
    reader.read_exact(&mut action_buf)?;
    let action = action_buf[0];

    // Validate action
    if action != ACTION_ADD && action != ACTION_REMOVE {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("Invalid manifest action: {}", action),
        ));
    }

    // Read level
    let mut level_buf = [0u8; 4];
    reader.read_exact(&mut level_buf)?;
    let level = u32::from_be_bytes(level_buf);

    // Read filename length
    let mut len_buf = [0u8; 8];
    reader.read_exact(&mut len_buf)?;
    let filename_len = u64::from_be_bytes(len_buf) as usize;

    // Read filename
    let mut filename_bytes = vec![0u8; filename_len];
    reader.read_exact(&mut filename_bytes)?;

    // Read checksum
    let mut checksum_buf = [0u8; 4];
    reader.read_exact(&mut checksum_buf)?;
    let stored_checksum = u32::from_be_bytes(checksum_buf);

    // Verify checksum
    let mut data_for_checksum = Vec::new();
    data_for_checksum.push(action);
    data_for_checksum.extend_from_slice(&level_buf);
    data_for_checksum.extend_from_slice(&len_buf);
    data_for_checksum.extend_from_slice(&filename_bytes);

    let calculated_checksum = crc32fast::hash(&data_for_checksum);
    if calculated_checksum != stored_checksum {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "Manifest entry checksum mismatch: expected 0x{:X}, got 0x{:X}",
                calculated_checksum, stored_checksum
            ),
        ));
    }

    let filename = String::from_utf8(filename_bytes).map_err(|e| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("Invalid UTF-8: {}", e),
        )
    })?;

    let entry = ManifestEntry {
        action,
        level,
        filename,
    };

    // Calculate next offset
    let next_offset = offset + 1 + 4 + 8 + filename_len as u64 + 4;

    Ok((entry, next_offset))
}

/// Add a file to the manifest
pub fn add_file(level: u32, filename: &str) -> Result<(), std::io::Error> {
    log::info!("Manifest: ADD level={} file={}", level, filename);

    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(MANIFEST_FILE)?;

    let entry = encode_manifest_entry(ACTION_ADD, level, filename);
    file.write_all(&entry)?;
    file.sync_data()?;

    Ok(())
}

/// Remove a file from the manifest
pub fn remove_file(level: u32, filename: &str) -> Result<(), std::io::Error> {
    log::info!("Manifest: REMOVE level={} file={}", level, filename);

    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(MANIFEST_FILE)?;

    let entry = encode_manifest_entry(ACTION_REMOVE, level, filename);
    file.write_all(&entry)?;
    file.sync_data()?;

    Ok(())
}

/// Read and replay the manifest to build the level store
/// Returns a HashMap where key is level and value is a list of filenames
pub fn read_manifest() -> Result<HashMap<u32, Vec<String>>, std::io::Error> {
    log::info!("Reading manifest file");

    // Check if manifest file exists
    let file = match OpenOptions::new().read(true).open(MANIFEST_FILE) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            log::info!("Manifest file not found, returning empty level store");
            return Ok(HashMap::new());
        }
        Err(e) => return Err(e),
    };

    let file_size = file.metadata()?.len();
    if file_size == 0 {
        log::info!("Manifest file is empty");
        return Ok(HashMap::new());
    }

    // Replay all manifest entries
    let mut level_store: HashMap<u32, Vec<String>> = HashMap::new();
    let mut offset = 0u64;
    let mut entry_count = 0;

    while offset < file_size {
        match decode_manifest_entry(&file, offset) {
            Ok((entry, next_offset)) => {
                entry_count += 1;

                match entry.action {
                    ACTION_ADD => {
                        log::debug!(
                            "Manifest replay: ADD level={} file={}",
                            entry.level,
                            entry.filename
                        );
                        level_store
                            .entry(entry.level)
                            .or_insert_with(Vec::new)
                            .push(entry.filename);
                    }
                    ACTION_REMOVE => {
                        log::debug!(
                            "Manifest replay: REMOVE level={} file={}",
                            entry.level,
                            entry.filename
                        );
                        if let Some(files) = level_store.get_mut(&entry.level) {
                            files.retain(|f| f != &entry.filename);
                            if files.is_empty() {
                                level_store.remove(&entry.level);
                            }
                        }
                    }
                    _ => {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!("Unknown action: {}", entry.action),
                        ));
                    }
                }

                offset = next_offset;
            }
            Err(e) => {
                if e.kind() == std::io::ErrorKind::UnexpectedEof {
                    log::warn!(
                        "Manifest file truncated at offset {}, stopping replay",
                        offset
                    );
                    break;
                }
                return Err(e);
            }
        }
    }

    log::info!(
        "Manifest replay complete: {} entries processed, {} levels with files",
        entry_count,
        level_store.len()
    );

    // Log summary of each level
    for (level, files) in level_store.iter() {
        log::info!("  Level {}: {} files", level, files.len());
    }

    Ok(level_store)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_encode_decode_manifest_entry() {
        let action = ACTION_ADD;
        let level = 0;
        let filename = "app-L0-1234567890.db";

        let encoded = encode_manifest_entry(action, level, filename);
        let (decoded, next_offset) = decode_manifest_entry(Cursor::new(&encoded), 0).unwrap();

        assert_eq!(decoded.action, action);
        assert_eq!(decoded.level, level);
        assert_eq!(decoded.filename, filename);
        assert_eq!(next_offset, encoded.len() as u64);
    }

    #[test]
    fn test_encode_decode_remove_entry() {
        let action = ACTION_REMOVE;
        let level = 1;
        let filename = "app-L1-9876543210.db";

        let encoded = encode_manifest_entry(action, level, filename);
        let (decoded, _) = decode_manifest_entry(Cursor::new(&encoded), 0).unwrap();

        assert_eq!(decoded.action, ACTION_REMOVE);
        assert_eq!(decoded.level, level);
        assert_eq!(decoded.filename, filename);
    }

    #[test]
    fn test_manifest_replay() {
        let test_manifest = "TEST_MANIFEST";
        let _ = std::fs::remove_file(test_manifest);

        // Simulate manifest operations
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(test_manifest)
            .unwrap();

        // Add files
        file.write_all(&encode_manifest_entry(ACTION_ADD, 0, "file1.db"))
            .unwrap();
        file.write_all(&encode_manifest_entry(ACTION_ADD, 0, "file2.db"))
            .unwrap();
        file.write_all(&encode_manifest_entry(ACTION_ADD, 1, "file3.db"))
            .unwrap();

        // Remove a file
        file.write_all(&encode_manifest_entry(ACTION_REMOVE, 0, "file1.db"))
            .unwrap();

        drop(file);

        // Read manifest using the test file
        let file = OpenOptions::new().read(true).open(test_manifest).unwrap();
        let file_size = file.metadata().unwrap().len();

        let mut level_store: HashMap<u32, Vec<String>> = HashMap::new();
        let mut offset = 0u64;

        while offset < file_size {
            let (entry, next_offset) = decode_manifest_entry(&file, offset).unwrap();
            match entry.action {
                ACTION_ADD => {
                    level_store
                        .entry(entry.level)
                        .or_insert_with(Vec::new)
                        .push(entry.filename);
                }
                ACTION_REMOVE => {
                    if let Some(files) = level_store.get_mut(&entry.level) {
                        files.retain(|f| f != &entry.filename);
                    }
                }
                _ => {}
            }
            offset = next_offset;
        }

        // Verify results
        assert_eq!(level_store.get(&0).unwrap(), &vec!["file2.db".to_string()]);
        assert_eq!(level_store.get(&1).unwrap(), &vec!["file3.db".to_string()]);

        // Cleanup
        std::fs::remove_file(test_manifest).unwrap();
    }
}
