use crate::error::DBError;

pub trait KVEngine {
    fn get(&self, key: &[u8]) -> Result<Vec<u8>, DBError>;
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), DBError>;
    fn delete(&mut self, key: &[u8]);
}
