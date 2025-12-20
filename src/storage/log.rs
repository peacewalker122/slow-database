use std::{
    fs::OpenOptions,
    io::{Read, Write},
};

#[derive(Debug)]
struct DecodeRecordResult {
    key: Vec<u8>,
    val: Vec<u8>,
    offset: u64,
}

pub fn store_log(filename: &str, key: &[u8], value: &[u8]) -> Result<(), std::io::Error> {
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(filename)?;

    let record = encode_record(key, value);
    file.write_all(&record)?;

    Ok(())
}

fn encode_record(key: &[u8], value: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(4 + 4 + key.len() + value.len() + 4);

    let checksum = crc32fast::hash(value);

    buf.extend_from_slice(&(key.len() as u64).to_be_bytes());
    buf.extend_from_slice(&(value.len() as u64).to_be_bytes());

    buf.extend_from_slice(key);
    buf.extend_from_slice(value);

    buf.extend_from_slice(&checksum.to_be_bytes());

    buf
}

pub fn decode_record(record: impl Read) -> Result<Box<DecodeRecordResult>, std::io::Error> {
    let mut reader = record;

    let mut len_buf = [0u8; 8];

    reader.read_exact(&mut len_buf)?;
    let key_len = u64::from_be_bytes(len_buf) as usize;

    reader.read_exact(&mut len_buf)?;
    let value_len = u64::from_be_bytes(len_buf) as usize;

    let mut key = vec![0u8; key_len];
    reader.read_exact(&mut key)?;

    let mut value = vec![0u8; value_len];
    reader.read_exact(&mut value)?;

    let mut crc_buf = [0u8; 4];
    reader.read_exact(&mut crc_buf)?;
    let checksum = u32::from_be_bytes(crc_buf);

    if crc32fast::hash(&value) != checksum {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Checksum mismatch",
        ));
    }

    let result = Box::new(DecodeRecordResult {
        key,
        val: value,
        offset: (16 + key_len + value_len + 4) as u64,
    });

    return Ok(result);
}

#[cfg(test)]
mod tests {
    use std::fs::File;

    use super::*;

    #[test]
    fn test_decode_log() {
        let data: &[u8] = &encode_record(b"key0", b"value0");

        let result = decode_record(data).unwrap();

        assert_eq!(result.key, b"key0");
        assert_eq!(result.val, b"value0");
        assert_eq!(result.offset, 30);
    }

    #[test]
    fn test_integration_decode_log() {
        let file = File::open("app.log").unwrap();

        let result = decode_record(file).unwrap();

        assert_eq!(result.key, b"key1");
        assert_eq!(result.val, b"value1");
        assert_eq!(result.offset, 30);
    }
}
