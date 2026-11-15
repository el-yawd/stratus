//! In-memory storage implementations.

use parking_lot::RwLock;
use parking_lot::RwLockUpgradableReadGuard;
#[cfg(not(feature = "dev"))]
use parking_lot::RwLockWriteGuard;
use rustc_hash::FxHashSet;

use crate::eth::executor::State;
use crate::eth::executor::TransactionExecution;
use crate::eth::executor::TransactionExecutionInput;
use crate::eth::executor::types::state::Complete;
#[cfg(feature = "dev")]
use crate::eth::executor::types::state::CompleteValue;
use crate::eth::storage::StorageError;
use crate::eth::storage::temporary::inmemory::InMemoryTemporaryStorageState;
use crate::eth::storage::temporary::inmemory::recent_transactions::RecentTransactions;
use crate::eth::types::Account;
use crate::eth::types::Address;
use crate::eth::types::BlockInfo;
use crate::eth::types::BlockNumber;
#[cfg(feature = "dev")]
use crate::eth::types::Bytes;
use crate::eth::types::Hash;
#[cfg(feature = "dev")]
use crate::eth::types::Nonce;
use crate::eth::types::PendingBlock;
use crate::eth::types::Slot;
use crate::eth::types::SlotIndex;
use crate::eth::types::TransactionInput;
use crate::eth::types::UnixTime;
#[cfg(feature = "dev")]
use crate::eth::types::UnixTimeNow;
#[cfg(feature = "dev")]
use crate::eth::types::Wei;

#[derive(Debug)]
pub struct InmemoryTransactionTemporaryStorage {
    pub pending_block: RwLock<InMemoryTemporaryStorageState>,
    pub latest_block: RwLock<Option<InMemoryTemporaryStorageState>>,
    /// Transaction hashes of the in-flight block (finished, not yet saved) and of the last committed blocks.
    pub recent_transactions: RwLock<RecentTransactions>,
}

impl InmemoryTransactionTemporaryStorage {
    pub fn new(block_number: BlockNumber, recent_transactions_blocks: usize) -> Self {
        Self {
            pending_block: RwLock::new(InMemoryTemporaryStorageState {
                block: PendingBlock::new_at_now(block_number),
                state: State::default(),
            }),
            latest_block: RwLock::new(None),
            recent_transactions: RwLock::new(RecentTransactions::new(recent_transactions_blocks)),
        }
    }

    pub fn set_pending_header(&self, number: BlockNumber, timestamp: UnixTime) {
        let mut pending_block = self.pending_block.write();
        pending_block.block.header.number = number;
        pending_block.block.header.timestamp = timestamp.into();
    }

    // -------------------------------------------------------------------------
    // Block number
    // -------------------------------------------------------------------------

    pub fn read_pending_block_header(&self) -> BlockInfo {
        let pending_block = self.pending_block.read();
        pending_block.block.header
    }

    #[cfg(feature = "dev")]
    pub fn set_pending_block_header(&self, block_number: BlockNumber) -> anyhow::Result<(), StorageError> {
        self.pending_block.write().block.header.number = block_number;
        Ok(())
    }

    // -------------------------------------------------------------------------
    // Block and executions
    // -------------------------------------------------------------------------

    pub fn save_pending_execution(&self, tx: TransactionExecution, state: State<Complete>) -> Result<(), StorageError> {
        // check conflicts
        let pending_block = self.pending_block.upgradable_read();
        if tx.input != pending_block.block.header {
            let actual_input = tx.input.clone();
            let tx_input: TransactionInput = tx.into();
            let expected_input = TransactionExecutionInput::create(&tx_input, pending_block.block.header);
            return Err(StorageError::EvmInputMismatch {
                expected: Box::new(expected_input),
                actual: Box::new(actual_input),
            });
        }

        let mut pending_block = RwLockUpgradableReadGuard::<InMemoryTemporaryStorageState>::upgrade(pending_block);

        pending_block.state.merge(state);

        // save execution
        pending_block.block.push_transaction(tx);

        Ok(())
    }

    pub fn read_pending_executions(&self) -> Vec<TransactionExecution> {
        self.pending_block.read().block.transactions.iter().map(|(_, tx)| tx.clone()).collect()
    }

    pub fn clone_pending_state(&self) -> InMemoryTemporaryStorageState {
        let pending_block = self.pending_block.read();
        (*pending_block).clone()
    }

    pub fn finish_pending_block(&self) -> (PendingBlock, State<Complete>) {
        let pending_block = self.pending_block.upgradable_read();

        // Mark the transactions as in flight BEFORE removing them from the pending block, so that a reader that misses
        // in the pending block (see `contains_recent_transaction`) is guaranteed to find them in the in-flight block.
        // The set is built before taking the write lock to keep readers blocked for as short as possible.
        let in_flight: FxHashSet<Hash> = pending_block.block.transactions.keys().copied().collect();
        self.recent_transactions.write().set_in_flight(pending_block.block.header.number, in_flight);

        // This has to happen BEFORE creating the new state, because UnixTimeNow::default() may change the offset.
        #[cfg(feature = "dev")]
        let (finished_block, state) = {
            let mut finished_block = pending_block.block.clone();
            // Update block timestamp only if evm_setNextBlockTimestamp was called,
            // otherwise keep the original timestamp from pending block creation
            if UnixTime::evm_set_next_block_timestamp_was_called() {
                finished_block.header.timestamp = UnixTimeNow::default();
            }
            (finished_block, pending_block.state.clone())
        };

        let next_state = InMemoryTemporaryStorageState::new(pending_block.block.header.number.next_block_number());

        let mut pending_block = RwLockUpgradableReadGuard::<InMemoryTemporaryStorageState>::upgrade(pending_block);
        let mut latest = self.latest_block.write();

        *latest = Some(std::mem::replace(&mut *pending_block, next_state));

        drop(pending_block);

        #[cfg(not(feature = "dev"))]
        let (finished_block, state) = {
            let latest = RwLockWriteGuard::<Option<InMemoryTemporaryStorageState>>::downgrade(latest);

            #[allow(clippy::expect_used)]
            let latest_state = latest.as_ref().expect("latest should be Some after finishing the pending block");
            (latest_state.block.clone(), latest_state.state.clone())
        };

        (finished_block, state)
    }

    pub fn read_pending_execution(&self, hash: Hash) -> Option<TransactionExecution> {
        let pending_block = self.pending_block.read();
        pending_block.block.transactions.get(&hash).cloned()
    }

    /// Checks whether the transaction is in the pending block, in the in-flight block or in one of the most recently
    /// committed blocks.
    ///
    /// The pending block MUST be checked before the recent transactions: hashes are marked in flight before being
    /// removed from the pending block, so checking in this order never misses a transaction that is being mined.
    pub fn contains_recent_transaction(&self, hash: Hash) -> bool {
        self.contains_recent_transaction_with(hash, || {})
    }

    /// [`Self::contains_recent_transaction`] with a hook that runs between the two checks, so tests can
    /// deterministically interleave a writer there.
    fn contains_recent_transaction_with(&self, hash: Hash, between_checks: impl FnOnce()) -> bool {
        if self.pending_block.read().block.transactions.contains_key(&hash) {
            return true;
        }
        between_checks();
        self.recent_transactions.read().contains(&hash)
    }

    pub fn recent_transactions_blocks(&self) -> usize {
        self.recent_transactions.read().capacity_blocks()
    }

    /// Adds the transaction hashes of a block saved to the permanent storage to the committed window, clearing it from
    /// the in-flight block.
    pub fn commit_recent_transactions(&self, number: BlockNumber, hashes: impl IntoIterator<Item = Hash>) {
        self.recent_transactions.write().commit_block(number, hashes);
    }

    /// Discards the in-flight block if it has the given number, because it was not saved.
    pub fn discard_in_flight_transactions(&self, number: BlockNumber) {
        self.recent_transactions.write().discard_in_flight(number);
    }

    // -------------------------------------------------------------------------
    // Accounts and Slots
    // -------------------------------------------------------------------------

    pub fn read_account(&self, address: Address) -> Option<Account> {
        match self.pending_block.read().state.accounts.get(&address) {
            Some(pending_account) => Some(pending_account.clone().to_account(address)),
            None => self
                .latest_block
                .read()
                .as_ref()
                .and_then(|latest| latest.state.accounts.get(&address))
                .map(|account| account.clone().to_account(address)),
        }
    }

    pub fn read_slot(&self, address: Address, index: SlotIndex) -> Option<Slot> {
        match self.pending_block.read().state.slots.get(&(address, index)) {
            Some(pending_value) => Some(Slot::new(index, *pending_value.value())),
            None => self
                .latest_block
                .read()
                .as_ref()
                .and_then(|latest| latest.state.slots.get(&(address, index)).map(|value| Slot::new(index, *value.value()))),
        }
    }

    /// Retains only the addresses that are missing from both the pending and latest temporary states.
    ///
    /// Batched: each lock is acquired once for the whole key set, instead of once per key,
    /// to reduce contention with the executor.
    pub fn retain_missing_accounts(&self, addresses: &mut Vec<Address>) {
        {
            let pending_block = self.pending_block.read();
            addresses.retain(|address| !pending_block.state.accounts.contains_key(address));
        }
        let latest_block = self.latest_block.read();
        if let Some(latest_block) = latest_block.as_ref() {
            addresses.retain(|address| !latest_block.state.accounts.contains_key(address));
        }
    }

    /// Retains only the slot keys that are missing from both the pending and latest temporary states.
    ///
    /// Batched: each lock is acquired once for the whole key set, instead of once per key,
    /// to reduce contention with the executor.
    pub fn retain_missing_slots(&self, slot_keys: &mut Vec<(Address, SlotIndex)>) {
        {
            let pending_block = self.pending_block.read();
            slot_keys.retain(|slot_key| !pending_block.state.slots.contains_key(slot_key));
        }
        let latest_block = self.latest_block.read();
        if let Some(latest_block) = latest_block.as_ref() {
            slot_keys.retain(|slot_key| !latest_block.state.slots.contains_key(slot_key));
        }
    }

    // -------------------------------------------------------------------------
    // Direct state manipulation (for testing)
    // -------------------------------------------------------------------------

    #[cfg(feature = "dev")]
    pub fn save_slot(&self, address: Address, slot: Slot) -> anyhow::Result<(), StorageError> {
        let mut pending_block = self.pending_block.write();
        pending_block.state.slots.insert((address, slot.index), CompleteValue::Changed(slot.value));
        Ok(())
    }

    #[cfg(feature = "dev")]
    pub fn save_account_nonce(&self, address: Address, nonce: Nonce) -> anyhow::Result<(), StorageError> {
        let mut pending_block = self.pending_block.write();

        // Only update if the account exists
        if let Some(account) = pending_block.state.accounts.get_mut(&address) {
            account.nonce.apply(nonce);
        }

        Ok(())
    }

    #[cfg(feature = "dev")]
    pub fn save_account_balance(&self, address: Address, balance: Wei) -> anyhow::Result<(), StorageError> {
        let mut pending_block = self.pending_block.write();

        // Only update if the account exists
        if let Some(account) = pending_block.state.accounts.get_mut(&address) {
            account.balance.apply(balance);
        }

        Ok(())
    }

    #[cfg(feature = "dev")]
    pub fn save_account_code(&self, address: Address, code: Bytes) -> anyhow::Result<(), StorageError> {
        use crate::alias::RevmBytecode;

        let mut pending_block = self.pending_block.write();

        // Only update if the account exists
        if let Some(account) = pending_block.state.accounts.get_mut(&address) {
            account.bytecode.apply(if code.0.is_empty() {
                None
            } else {
                Some(RevmBytecode::new_raw(code.0.into()))
            });
        }

        Ok(())
    }

    // -------------------------------------------------------------------------
    // Global state
    // -------------------------------------------------------------------------
    pub fn reset(&self) {
        self.pending_block.write().reset();
        *self.latest_block.write() = None;
        self.recent_transactions.write().clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::eth::executor::TransactionExecutionResult;
    use crate::eth::storage::temporary::DEFAULT_RECENT_TRANSACTIONS_BLOCKS;
    use crate::eth::types::Signature;
    use crate::eth::types::TransactionInfo;

    fn storage_with_pending_transaction(hash: Hash) -> InmemoryTransactionTemporaryStorage {
        let storage = InmemoryTransactionTemporaryStorage::new(1.into(), DEFAULT_RECENT_TRANSACTIONS_BLOCKS);
        let evm_input = TransactionExecutionInput::create(&TransactionInput::default(), storage.read_pending_block_header());
        let result = TransactionExecutionResult {
            result: crate::eth::executor::ExecutionResult::Success,
            ..Default::default()
        };
        let info = TransactionInfo { hash, ..Default::default() };
        let tx = TransactionExecution::new(info, Signature::default(), evm_input, result);
        storage.save_pending_execution(tx, State::default()).expect("save pending execution");
        storage
    }

    #[test]
    fn reader_does_not_miss_a_block_finished_between_its_checks() {
        let hash = Hash::new([0x77; 32]);
        let storage = storage_with_pending_transaction(hash);

        // Finish the block between the reader's two checks. With the required order (pending first) the first check
        // already finds the tx; checking recent first would miss it there, and then miss it in the now empty pending.
        let found = storage.contains_recent_transaction_with(hash, || {
            storage.finish_pending_block();
        });
        assert!(found);
    }

    #[test]
    fn reader_sees_a_block_finished_before_its_checks() {
        let hash = Hash::new([0x78; 32]);
        let storage = storage_with_pending_transaction(hash);
        storage.finish_pending_block();
        assert!(storage.contains_recent_transaction(hash), "in-flight block");
    }
}
