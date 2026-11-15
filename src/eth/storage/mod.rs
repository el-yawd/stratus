//! Ethereum / EVM storage.

use cache::CacheConfig;
pub use cache::StorageCache;
pub use error::StorageError;
pub use permanent::PermanentStorageConfig;
pub use permanent::RocksPermanentStorage;
pub use stratus_storage::InFlightBlockGuard;
pub use stratus_storage::MinedPointInTime;
pub use stratus_storage::StratusStorage;
pub use temporary::InMemoryTemporaryStorage;
pub use temporary::TemporaryStorageConfig;
pub use types::FoundAt;
mod cache;
mod error;
pub mod permanent;
mod resolve_pending;
mod stratus_storage;
mod temporary;
mod types;

use std::sync::Arc;

use clap::Parser;
use display_json::DebugAsJson;
pub use temporary::compute_pending_block_number;

use crate::eth::types::StratusError;

// -----------------------------------------------------------------------------
// Config
// -----------------------------------------------------------------------------

/// Configuration that can be used by any binary that interacts with Stratus storage.
#[derive(Parser, DebugAsJson, Clone, serde::Serialize)]
#[cfg_attr(test, derive(Default))]
pub struct StorageConfig {
    #[clap(flatten)]
    #[serde(rename = "permanent")]
    pub perm_storage: PermanentStorageConfig,

    #[clap(flatten)]
    #[serde(rename = "temporary")]
    pub temp_storage: TemporaryStorageConfig,

    #[clap(flatten)]
    pub cache: CacheConfig,
}

impl StorageConfig {
    /// Initializes Stratus storage.
    pub fn init(&self) -> Result<Arc<StratusStorage>, StratusError> {
        let perm_storage = self.perm_storage.init()?;
        let temp_storage = InMemoryTemporaryStorage::from_perm_storage(&perm_storage, &self.temp_storage)?;
        let cache = self.cache.init();

        let storage = StratusStorage::new(
            temp_storage,
            perm_storage,
            cache,
            #[cfg(feature = "dev")]
            self.perm_storage.clone(),
        )?;

        Ok(Arc::new(storage))
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;
    use crate::eth::executor::State;
    use crate::eth::executor::TransactionExecution;
    use crate::eth::executor::TransactionExecutionInput;
    use crate::eth::executor::TransactionExecutionResult;
    use crate::eth::executor::types::state::AccountChanges;
    use crate::eth::executor::types::state::Complete;
    use crate::eth::executor::types::state::CompleteValue;
    use crate::eth::storage::cache::CacheConfig;
    use crate::eth::storage::temporary::DEFAULT_RECENT_TRANSACTIONS_BLOCKS;
    use crate::eth::types::Account;
    use crate::eth::types::Address;
    use crate::eth::types::BlockNumber;
    use crate::eth::types::Hash;
    use crate::eth::types::Signature;
    use crate::eth::types::SlotIndex;
    use crate::eth::types::SlotValue;
    use crate::eth::types::TargetState;
    use crate::eth::types::TransactionInfo;
    use crate::eth::types::TransactionInput;
    use crate::eth::types::Wei;

    impl StratusStorage {
        pub fn mine_block_with_mock_execution(&self, state: State<Complete>) -> BlockNumber {
            let header = self.read_pending_block_header();
            let evm_input = TransactionExecutionInput::create(&TransactionInput::default(), header);

            let result = TransactionExecutionResult {
                result: crate::eth::executor::ExecutionResult::Success,
                ..Default::default()
            };

            let tx = TransactionExecution::new(TransactionInfo::default(), Signature::default(), evm_input, result);
            self.save_execution(tx, state).expect("save execution");

            let (block, block_changes) = self.finish_pending_block();
            let block_number = block.header.number;
            self.save_block(block.into(), block_changes).expect("save block");
            block_number
        }

        /// Saves a successful mock transaction with the given hash to the pending block.
        pub fn save_mock_execution_with_hash(&self, hash: Hash) {
            let header = self.read_pending_block_header();
            let evm_input = TransactionExecutionInput::create(&TransactionInput::default(), header);
            let result = TransactionExecutionResult {
                result: crate::eth::executor::ExecutionResult::Success,
                ..Default::default()
            };
            let info = TransactionInfo { hash, ..Default::default() };
            let tx = TransactionExecution::new(info, Signature::default(), evm_input, result);
            self.save_execution(tx, State::default()).expect("save execution");
        }

        pub fn new_test() -> Result<Self, StorageError> {
            Self::new_test_with_temp_config(&TemporaryStorageConfig::default())
        }

        pub fn new_test_with_temp_config(temp_config: &TemporaryStorageConfig) -> Result<Self, StorageError> {
            let temp = InMemoryTemporaryStorage::new(0.into(), temp_config);

            // Create a temporary directory for RocksDB
            let rocks_dir = tempdir().expect("Failed to create temporary directory for tests");
            let rocks_path_prefix = rocks_dir.path().to_str().unwrap().to_string();

            let perm = RocksPermanentStorage::new(
                Some(rocks_path_prefix.clone()),
                std::time::Duration::from_secs(240),
                super::permanent::RocksCfCacheConfig::default(),
                true,
                #[cfg(feature = "metrics")]
                None,
                1024,
            )
            .expect("Failed to create RocksPermanentStorage for tests");

            let cache = CacheConfig {
                account_history_cache_capacity: 20000,
                slot_history_cache_capacity: 100000,
            }
            .init();

            Self::new(
                temp,
                perm,
                cache,
                #[cfg(feature = "dev")]
                super::permanent::PermanentStorageConfig {
                    rocks_path_prefix: Some(rocks_path_prefix),
                    rocks_shutdown_timeout: std::time::Duration::from_secs(240),
                    rocks_cf_cache: super::permanent::RocksCfCacheConfig::default(),
                    rocks_disable_sync_write: false,
                    rocks_cf_size_metrics_interval: None,
                    genesis_file: crate::config::GenesisFileConfig::default(),
                    rocks_file_descriptors_limit: 1024,
                },
            )
            .inspect(|this| {
                if !this.has_genesis().unwrap() {
                    this.mine_block_with_mock_execution(State::default());
                }
            })
        }
    }

    /// An `eth_call` pinned to a block that is no longer the latest must read the historical
    /// state at its captured block, not the current latest state.
    #[test]
    fn read_slot_for_call_pinned_to_older_block_must_not_read_latest_state() {
        let storage = StratusStorage::new_test().expect("failed to build test storage");

        let address = Address::new([0xAA; 20]);
        let index = SlotIndex::ZERO;

        // Mine a block setting slot S = 100. The eth_call captures this block.
        let mut changes1 = State::default();
        changes1
            .slots
            .insert((address, index), CompleteValue::Changed(SlotValue::from([100u64, 0, 0, 0])));
        let call_block = storage.mine_block_with_mock_execution(changes1);

        // A new block is mined while the call is in flight, changing the slot to 200.
        let mut changes2 = State::default();
        changes2
            .slots
            .insert((address, index), CompleteValue::Changed(SlotValue::from([200u64, 0, 0, 0])));
        let latest = storage.mine_block_with_mock_execution(changes2);
        assert_ne!(call_block, latest);

        // The in-flight call (pinned to the first block) reads the slot.
        let (slot, _) = storage.read_slot(address, index, TargetState::LatestPinned(call_block)).expect("read slot");

        // Must reflect the first block (100), not the freshly mined latest (200).
        assert_eq!(slot.value, SlotValue::from([100u64, 0, 0, 0]));
    }

    #[test]
    fn read_account_for_call_pinned_to_older_block_must_not_read_latest_state() {
        let storage = StratusStorage::new_test().expect("failed to build test storage");

        let address = Address::new([0xBB; 20]);

        // Mine a block setting the account balance to 100. The eth_call captures this block.
        let mut changes1 = State::default();
        changes1
            .accounts
            .insert(address, AccountChanges::from_changed(Account::new_with_balance(address, Wei::from(100u64))));
        let call_block = storage.mine_block_with_mock_execution(changes1);

        // A new block is mined while the call is in flight, changing the balance to 200.
        let mut changes2 = State::default();
        changes2
            .accounts
            .insert(address, AccountChanges::from_changed(Account::new_with_balance(address, Wei::from(200u64))));
        let latest = storage.mine_block_with_mock_execution(changes2);
        assert_ne!(call_block, latest);

        let (account, _) = storage.read_account(address, TargetState::LatestPinned(call_block)).expect("read account");

        // Must reflect the first block (100), not the freshly mined latest (200).
        assert_eq!(account.balance, Wei::from(100u64));
    }

    fn mine_pending_block(storage: &StratusStorage) {
        let (block, changes) = storage.finish_pending_block();
        storage.save_block(block.into(), changes).expect("save block");
    }

    #[test]
    fn transaction_exists_recent_covers_pending_and_last_mined_blocks() {
        let storage = StratusStorage::new_test().expect("failed to build test storage");
        let hash = Hash::new([0xAA; 32]);

        assert!(!storage.transaction_exists_recent(hash));

        storage.save_mock_execution_with_hash(hash);
        assert!(storage.transaction_exists_recent(hash), "pending transaction");

        // finished but not yet committed
        let (block, changes) = storage.finish_pending_block();
        assert!(storage.transaction_exists_recent(hash), "finished block, not yet saved");
        storage.save_block(block.into(), changes).expect("save block");
        assert!(storage.transaction_exists_recent(hash), "mined block");

        // still inside the window
        for _ in 1..DEFAULT_RECENT_TRANSACTIONS_BLOCKS {
            mine_pending_block(&storage);
        }
        assert!(storage.transaction_exists_recent(hash), "last block of the window");

        // pushed out of the window
        mine_pending_block(&storage);
        assert!(!storage.transaction_exists_recent(hash), "evicted from the window");
    }

    /// Finishes the pending block and rewinds the pending header so that saving it fails with a pending number
    /// conflict, then restores the header so a retry can succeed.
    fn finish_block_with_failing_save(storage: &Arc<StratusStorage>) -> (crate::eth::types::Block, State<Complete>, InFlightBlockGuard) {
        let (pending, changes) = storage.finish_pending_block();
        let in_flight = InFlightBlockGuard::new(Arc::clone(storage), pending.header.number);
        let block: crate::eth::types::Block = pending.into();

        storage.set_pending_header(block.number(), block.timestamp());
        storage
            .save_block(block.clone(), changes.clone())
            .expect_err("save must fail with a pending number conflict");
        storage.set_pending_header(block.number().next_block_number(), block.timestamp());

        (block, changes, in_flight)
    }

    #[test]
    fn transaction_exists_recent_is_kept_while_a_failed_save_is_retried() {
        let storage = Arc::new(StratusStorage::new_test().expect("failed to build test storage"));
        let hash = Hash::new([0xDD; 32]);
        storage.save_mock_execution_with_hash(hash);

        let (block, changes, in_flight) = finish_block_with_failing_save(&storage);
        assert!(storage.transaction_exists_recent(hash), "failed save must not hide a block that may be retried");

        storage.save_block(block, changes).expect("retry succeeds");
        assert!(storage.transaction_exists_recent(hash), "saved block");

        drop(in_flight);
        assert!(storage.transaction_exists_recent(hash), "dropping the guard of a saved block is a no-op");
    }

    #[test]
    fn transaction_exists_recent_ignores_abandoned_blocks() {
        let storage = Arc::new(StratusStorage::new_test().expect("failed to build test storage"));
        let hash = Hash::new([0xEE; 32]);
        storage.save_mock_execution_with_hash(hash);

        let (_, _, in_flight) = finish_block_with_failing_save(&storage);
        assert!(storage.transaction_exists_recent(hash));

        // the caller gives up on the block
        drop(in_flight);
        assert!(!storage.transaction_exists_recent(hash), "abandoned block was never saved");
    }

    #[test]
    fn transaction_exists_recent_with_zero_capacity_only_sees_pending_and_in_flight() {
        let config = TemporaryStorageConfig { recent_transactions_blocks: 0 };
        let storage = Arc::new(StratusStorage::new_test_with_temp_config(&config).expect("failed to build test storage"));
        let hash = Hash::new([0x0F; 32]);

        storage.save_mock_execution_with_hash(hash);
        let (pending, changes) = storage.finish_pending_block();
        assert!(storage.transaction_exists_recent(hash), "in-flight block");
        storage.save_block(pending.into(), changes).expect("save block");
        assert!(!storage.transaction_exists_recent(hash), "no committed window");

        storage.load_recent_transactions().expect("load recent transactions");
        assert!(!storage.transaction_exists_recent(hash), "reload must respect the zero capacity");
    }

    #[cfg(feature = "dev")]
    #[test]
    fn reset_to_genesis_clears_recent_transactions() {
        let storage = Arc::new(StratusStorage::new_test().expect("failed to build test storage"));
        let committed = Hash::new([0x1F; 32]);
        let in_flight = Hash::new([0x2F; 32]);

        storage.save_mock_execution_with_hash(committed);
        mine_pending_block(&storage);
        storage.save_mock_execution_with_hash(in_flight);
        let _ = storage.finish_pending_block();
        assert!(storage.transaction_exists_recent(committed));
        assert!(storage.transaction_exists_recent(in_flight));

        storage.reset_to_genesis().expect("reset to genesis");
        assert!(!storage.transaction_exists_recent(committed));
        assert!(!storage.transaction_exists_recent(in_flight));
    }

    #[test]
    fn transaction_exists_recent_is_loaded_from_permanent_storage() {
        let storage = StratusStorage::new_test().expect("failed to build test storage");
        let old = Hash::new([0x01; 32]);
        let recent = Hash::new([0x02; 32]);

        storage.save_mock_execution_with_hash(old);
        mine_pending_block(&storage);
        for _ in 1..DEFAULT_RECENT_TRANSACTIONS_BLOCKS {
            mine_pending_block(&storage);
        }
        storage.save_mock_execution_with_hash(recent);
        mine_pending_block(&storage);

        // simulate a restart: drop the in-memory window and rebuild it from the permanent storage
        storage.temp.transaction_storage.recent_transactions.write().clear();
        assert!(!storage.transaction_exists_recent(recent));
        storage.load_recent_transactions().expect("load recent transactions");

        assert!(storage.transaction_exists_recent(recent));
        assert!(!storage.transaction_exists_recent(old), "outside of the window");
    }
}
