use clap::Parser;
use display_json::DebugAsJson;
pub use inmemory::DEFAULT_RECENT_TRANSACTIONS_BLOCKS;
pub use inmemory::InMemoryTemporaryStorage;
pub use inmemory::MAX_RECENT_TRANSACTIONS_BLOCKS;

mod inmemory;

use super::RocksPermanentStorage;
use crate::eth::types::BlockNumber;

/// Configuration of the in-memory temporary storage.
#[derive(Parser, DebugAsJson, Clone, serde::Serialize)]
pub struct TemporaryStorageConfig {
    /// Number of most recent mined blocks whose transaction hashes are kept in memory for fast existence checks.
    /// With 0, only the pending and in-flight (finished, being saved) blocks are checked. These blocks are read one by
    /// one from the permanent storage on startup, so the value is capped at MAX_RECENT_TRANSACTIONS_BLOCKS.
    #[arg(
        id = "storage.temporary.recent_transactions_blocks",
        long = "recent-transactions-blocks",
        default_value_t = DEFAULT_RECENT_TRANSACTIONS_BLOCKS,
        value_parser = clap::builder::RangedU64ValueParser::<usize>::new().range(0..=MAX_RECENT_TRANSACTIONS_BLOCKS as u64)
    )]
    pub recent_transactions_blocks: usize,
}

#[cfg(test)]
impl Default for TemporaryStorageConfig {
    fn default() -> Self {
        Self {
            recent_transactions_blocks: DEFAULT_RECENT_TRANSACTIONS_BLOCKS,
        }
    }
}

pub fn compute_pending_block_number(perm_storage: &RocksPermanentStorage) -> anyhow::Result<BlockNumber> {
    let mined_block_number = perm_storage.read_mined_block_number();
    Ok(if !perm_storage.has_genesis()? && mined_block_number == BlockNumber::ZERO {
        BlockNumber::ZERO
    } else {
        mined_block_number + 1
    })
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::*;

    #[test]
    fn recent_transactions_blocks_is_bounded() {
        let parse = |value: usize| TemporaryStorageConfig::try_parse_from(["stratus", "--recent-transactions-blocks", &value.to_string()]);
        assert_eq!(parse(0).unwrap().recent_transactions_blocks, 0);
        assert_eq!(
            parse(MAX_RECENT_TRANSACTIONS_BLOCKS).unwrap().recent_transactions_blocks,
            MAX_RECENT_TRANSACTIONS_BLOCKS
        );
        assert!(parse(MAX_RECENT_TRANSACTIONS_BLOCKS + 1).is_err());
    }
}
