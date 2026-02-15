use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Command {
    Put {
        key: Vec<u8>,
        value: Vec<u8>,
    },
    Delete {
        key: Vec<u8>,
    },
    CreateCollection {
        name: String,
        num_partitions: u32,
        replication_factor: u32,
    },
    DropCollection {
        name: String,
    },
}

impl Command {
    pub fn serialize(&self) -> Vec<u8> {
        bincode::serde::encode_to_vec(self, bincode::config::standard()).unwrap()
    }

    pub fn deserialize(data: &[u8]) -> Option<Self> {
        let (command, _len): (Self, usize) =
            bincode::serde::decode_from_slice(data, bincode::config::standard()).ok()?;
        Some(command)
    }
}
