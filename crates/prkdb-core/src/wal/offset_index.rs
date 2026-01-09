use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::Path;
use std::sync::{Arc, Mutex};

/// Entry in the offset index
#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct IndexEntry {
    /// Offset relative to segment base_offset
    pub relative_offset: u32,

    /// Physical position in log file
    pub position: u32,
}

impl IndexEntry {
    pub fn to_bytes(&self) -> [u8; 8] {
        let mut bytes = [0u8; 8];
        bytes[0..4].copy_from_slice(&self.relative_offset.to_le_bytes());
        bytes[4..8].copy_from_slice(&self.position.to_le_bytes());
        bytes
    }

    pub fn from_bytes(bytes: &[u8]) -> Self {
        assert!(bytes.len() >= 8, "need 8 bytes for IndexEntry");
        Self {
            relative_offset: u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]),
            position: u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]),
        }
    }
}

/// Sparse offset index mapping offsets to file positions
pub struct OffsetIndex {
    base_offset: u64,
    file: Arc<Mutex<File>>,
    entries: Vec<IndexEntry>,
    max_entries: usize,
}

impl OffsetIndex {
    /// Create a new offset index
    pub fn create(path: &Path, base_offset: u64, max_entries: usize) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;

        Ok(Self {
            base_offset,
            file: Arc::new(Mutex::new(file)),
            entries: Vec::new(),
            max_entries,
        })
    }

    /// Open an existing offset index
    pub fn open(path: &Path, base_offset: u64, max_entries: usize) -> io::Result<Self> {
        let mut file = OpenOptions::new().read(true).write(true).open(path)?;

        // Read all existing entries
        let mut entries = Vec::new();
        let mut buffer = [0u8; 8];

        while let Ok(8) = file.read(&mut buffer) {
            entries.push(IndexEntry::from_bytes(&buffer));
        }

        Ok(Self {
            base_offset,
            file: Arc::new(Mutex::new(file)),
            entries,
            max_entries,
        })
    }

    /// Add an index entry
    pub fn append(&mut self, offset: u64, position: u64) -> io::Result<()> {
        if self.entries.len() >= self.max_entries {
            return Err(io::Error::new(io::ErrorKind::Other, "index full"));
        }

        let entry = IndexEntry {
            relative_offset: (offset - self.base_offset) as u32,
            position: position as u32,
        };

        let mut file = self.file.lock().unwrap();
        file.write_all(&entry.to_bytes())?;
        file.sync_all()?;

        self.entries.push(entry);

        Ok(())
    }

    /// Find the file position for a given offset
    /// Returns the position of the closest entry <= offset
    pub fn lookup(&self, offset: u64) -> Option<u64> {
        if offset < self.base_offset {
            return None;
        }

        let relative = (offset - self.base_offset) as u32;

        // Binary search for closest entry <= offset
        match self
            .entries
            .binary_search_by_key(&relative, |e| e.relative_offset)
        {
            Ok(idx) => Some(self.entries[idx].position as u64),
            Err(0) => None, // Before first entry
            Err(idx) => Some(self.entries[idx - 1].position as u64),
        }
    }

    /// Get base offset
    pub fn base_offset(&self) -> u64 {
        self.base_offset
    }

    /// Number of entries
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Check if index is empty
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    #[test]
    fn test_index_entry_serialization() {
        let entry = IndexEntry {
            relative_offset: 42,
            position: 1024,
        };

        let bytes = entry.to_bytes();
        let decoded = IndexEntry::from_bytes(&bytes);

        assert_eq!(entry.relative_offset, decoded.relative_offset);
        assert_eq!(entry.position, decoded.position);
    }

    #[test]
    fn test_offset_index() -> io::Result<()> {
        let dir = env::temp_dir();
        let path = dir.join("test_offset_index.idx");

        // Create index
        let mut index = OffsetIndex::create(&path, 1000, 100)?;

        // Add entries
        index.append(1000, 0)?;
        index.append(1100, 4096)?;
        index.append(1200, 8192)?;

        // Lookup
        assert_eq!(index.lookup(1000), Some(0));
        assert_eq!(index.lookup(1150), Some(4096)); // Falls back to 1100
        assert_eq!(index.lookup(1200), Some(8192));
        assert_eq!(index.lookup(999), None); // Before base

        // Clean up
        std::fs::remove_file(&path)?;

        Ok(())
    }
}
