use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::time::Duration;

use anyhow::anyhow;
use parking_lot::Mutex;
use parking_lot::MutexGuard;
use parking_lot::RwLock;
use stratus_metrics::timed;
use tokio::sync::Mutex as AsyncMutex;
use tokio::sync::broadcast;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::Span;

use crate::eth::executor::State;
use crate::eth::executor::TransactionExecution;
use crate::eth::executor::types::state::Complete;
use crate::eth::miner::MinerMode;
use crate::eth::storage::InFlightBlockGuard;
use crate::eth::storage::StorageError;
use crate::eth::storage::StratusStorage;
use crate::eth::types::Block;
use crate::eth::types::BlockHeader;
use crate::eth::types::ExternalBlock;
use crate::eth::types::Hash;
use crate::eth::types::LogMessage;
use crate::eth::types::StratusError;
use crate::ext::DisplayExt;
use crate::ext::not;
use crate::globals::STRATUS_SHUTDOWN_SIGNAL;
use crate::infra::tracing::SpanExt;

cfg_if::cfg_if! {
    if #[cfg(feature = "tracing")] {
        use tracing::field;
        use tracing::info_span;
    }
}

/// Represents different types of items that can be committed to storage
#[allow(clippy::large_enum_variant)]
#[derive(strum::AsRefStr)]
pub enum CommitItem {
    /// A block
    Block(Block),
    /// A block that wasn't executed in this node and instead contains all changes already pre-computed
    ReplicationBlock(Block),
}

pub struct Miner {
    pub locks: MinerLocks,

    storage: Arc<StratusStorage>,

    /// Miner is enabled by default, but can be disabled.
    is_paused: AtomicBool,

    /// Mode the block miner is running.
    mode: RwLock<MinerMode>,

    /// Broadcasts pending transactions events.
    pub notifier_pending_txs: broadcast::Sender<Hash>,

    /// Broadcasts new mined blocks events.
    pub notifier_blocks: broadcast::Sender<BlockHeader>,

    /// Broadcasts transaction logs events.
    pub notifier_logs: broadcast::Sender<LogMessage>,

    // -------------------------------------------------------------------------
    // Shutdown
    // -------------------------------------------------------------------------
    /// Signal sent to tasks to shutdown.
    shutdown_signal: Mutex<CancellationToken>,

    /// Spawned tasks for interval miner, can be used to await for complete shutdown.
    interval_joinset: AsyncMutex<Option<JoinSet<()>>>,
}

/// Locks used in operations that mutate state.
#[derive(Default)]
pub struct MinerLocks {
    save_execution: Mutex<()>,
    pub mine_and_commit: Mutex<()>,
    mine: Mutex<()>,
    commit: Mutex<()>,
}

impl Miner {
    /// Acquires all locks that mutate the pending/mined state, so a storage reset can never interleave with a
    /// transaction save, a block mine or a block commit. Otherwise, a concurrent mine can permanently desync the
    /// pending and mined block numbers (e.g. mined number reset to zero while the pending block already advanced).
    pub fn lock_state_for_reset(&self) -> (MutexGuard<'_, ()>, MutexGuard<'_, ()>, MutexGuard<'_, ()>, MutexGuard<'_, ()>) {
        // acquire in the same order used by the mining paths to avoid deadlocks
        let save_execution = self.locks.save_execution.lock();
        let mine_and_commit = self.locks.mine_and_commit.lock();
        let mine = self.locks.mine.lock();
        let commit = self.locks.commit.lock();
        (save_execution, mine_and_commit, mine, commit)
    }

    pub fn new(storage: Arc<StratusStorage>, mode: MinerMode) -> Self {
        tracing::info!(?mode, "creating block miner");
        Self {
            locks: MinerLocks::default(),
            storage,
            is_paused: AtomicBool::new(false),
            mode: mode.into(),
            notifier_pending_txs: broadcast::channel(u16::MAX as usize).0,
            notifier_blocks: broadcast::channel(u16::MAX as usize).0,
            notifier_logs: broadcast::channel(u16::MAX as usize).0,
            shutdown_signal: Mutex::new(STRATUS_SHUTDOWN_SIGNAL.child_token()),
            interval_joinset: AsyncMutex::new(None),
        }
    }

    /// Spawns a new thread that keep mining blocks in the specified interval.
    ///
    /// Also unpauses `Miner` if it was paused.
    pub async fn start_interval_mining(self: &Arc<Self>, block_time: Duration) {
        if self.is_interval_miner_running() {
            tracing::warn!(block_time = ?block_time.to_string_ext(), "tried to start interval mining, but it's already running, skipping");
            return;
        };

        tracing::info!(block_time = ?block_time.to_string_ext(), "spawning interval miner");
        self.set_mode(MinerMode::Interval(block_time));
        self.unpause();

        // spawn miner and ticker
        let (ticks_tx, ticks_rx) = mpsc::channel();
        let new_shutdown_signal = STRATUS_SHUTDOWN_SIGNAL.child_token();
        let mut joinset = JoinSet::new();

        joinset.spawn_blocking({
            let shutdown = new_shutdown_signal.clone();
            let miner_clone = Arc::clone(self);
            || interval_miner::run(miner_clone, ticks_rx, shutdown)
        });

        joinset.spawn(interval_miner_ticker::run(block_time, ticks_tx, new_shutdown_signal.clone()));

        *self.shutdown_signal.lock() = new_shutdown_signal;
        *self.interval_joinset.lock().await = Some(joinset);
    }

    /// Shuts down interval miner, set miner mode to External.
    pub async fn switch_to_external_mode(self: &Arc<Self>) {
        if self.mode().is_external() {
            tracing::warn!("trying to change mode to external, but it's already set, skipping");
            return;
        }
        self.shutdown_and_wait().await;
        self.set_mode(MinerMode::External);
        self.unpause();
    }

    // Unpause interval miner (if in interval mode)
    pub fn unpause(&self) {
        self.is_paused.store(false, Ordering::Relaxed);
    }

    // Pause interval miner (if in interval mode)
    pub fn pause(&self) {
        self.is_paused.store(true, Ordering::Relaxed);
    }

    // Whether or not interval miner is paused (means nothing if not in interval mode)
    pub fn is_paused(&self) -> bool {
        self.is_paused.load(Ordering::Relaxed)
    }

    pub fn mode(&self) -> MinerMode {
        *self.mode.read()
    }

    fn set_mode(&self, new_mode: MinerMode) {
        *self.mode.write() = new_mode;
    }

    pub fn is_interval_miner_running(&self) -> bool {
        match self.interval_joinset.try_lock() {
            // check if the joinset of tasks has futures running
            Ok(joinset) => joinset.as_ref().is_some_and(|joinset| not(joinset.is_empty())),
            // if the joinset is locked, it's either trying to shutdown or turning on, so yes
            Err(_) => true,
        }
    }

    /// Shutdown if miner is interval miner.
    async fn shutdown_and_wait(&self) {
        // Note: we are intentionally holding this mutex till the end of the function, so that
        // subsequent calls wait for the first to finish, and `is_interval_miner_running` works too
        let mut joinset_lock = self.interval_joinset.lock().await;

        let Some(mut joinset) = joinset_lock.take() else {
            return;
        };

        tracing::warn!("shutting down interval miner to switch to external mode");

        self.shutdown_signal.lock().cancel();

        // wait for all tasks to end
        while let Some(result) = joinset.join_next().await {
            if let Err(e) = result {
                tracing::error!(reason = ?e, "miner task failed");
            }
        }
    }

    /// Persists a transaction execution.
    pub fn save_execution(&self, tx_execution: TransactionExecution, state: State<Complete>) -> Result<(), StratusError> {
        let tx_hash = tx_execution.info.hash;

        // track
        #[cfg(feature = "tracing")]
        let _span = info_span!("miner::save_execution", %tx_hash).entered();

        // Check if automine is enabled
        let is_automine = self.mode().is_automine();

        // if automine is enabled, only one transaction can enter the block at a time.
        let _save_execution_lock = if is_automine { Some(self.locks.save_execution.lock()) } else { None };

        // save execution to temporary storage
        self.storage.save_execution(tx_execution, state)?;

        // notify
        if self.has_pending_tx_subscribers() {
            self.send_pending_tx_notification(&Some(tx_hash));
        }

        // if automine is enabled, automatically mines a block
        if is_automine {
            self.mine_local_and_commit()?;
        }

        Ok(())
    }

    /// Mines external block and external transactions.
    ///
    /// Local transactions are not allowed to be part of the block.
    pub fn mine_external(&self, external_block: ExternalBlock) -> anyhow::Result<(Block, State<Complete>, InFlightBlockGuard)> {
        // track
        #[cfg(feature = "tracing")]
        let _span = info_span!("miner::mine_external", block_number = field::Empty).entered();

        // lock
        let _mine_lock = self.locks.mine.lock();

        // mine block
        let (pending_block, changes) = self.storage.finish_pending_block();
        // on a mismatch the guard is dropped with the error, discarding the block's in-flight transactions
        let in_flight = InFlightBlockGuard::new(Arc::clone(&self.storage), pending_block.header.number);
        let mut block: Block = pending_block.into();

        Span::with(|s| s.rec_str("block_number", &block.header.number));
        block.apply_external(&external_block);

        match external_block == block {
            true => Ok((block, changes, in_flight)),
            false => Err(anyhow!(
                "mismatching block info:\n\tlocal:\n\t\tnumber: {:?}\n\t\ttimestamp: {:?}\n\t\thash: {:?}\n\texternal:\n\t\tnumber: {:?}\n\t\ttimestamp: {:?}\n\t\thash: {:?}",
                block.number(),
                block.header.timestamp,
                block.hash(),
                external_block.number(),
                external_block.timestamp(),
                external_block.hash()
            )),
        }
    }

    /// Same as [`Self::mine_local`], but automatically commits the block instead of returning it.
    /// mainly used when is_automine is enabled.
    pub fn mine_local_and_commit(&self) -> anyhow::Result<(), StorageError> {
        let _mine_and_commit_lock = self.locks.mine_and_commit.lock();

        let (block, changes, _in_flight) = self.mine_local();
        self.commit(CommitItem::Block(block), changes)
    }

    /// Mines local transactions.
    ///
    /// External transactions are not allowed to be part of the block.
    ///
    /// The returned [`InFlightBlockGuard`] must be held until the block is saved or abandoned.
    #[timed(miner_mine_local)]
    pub fn mine_local(&self) -> (Block, State<Complete>, InFlightBlockGuard) {
        #[cfg(feature = "tracing")]
        let _span = info_span!("miner::mine_local", block_number = field::Empty).entered();

        // lock
        let _mine_lock = self.locks.mine.lock();

        // mine block
        let (block, changes) = self.storage.finish_pending_block();
        Span::with(|s| s.rec_str("block_number", &block.header.number));
        let in_flight = InFlightBlockGuard::new(Arc::clone(&self.storage), block.header.number);

        (block.into(), changes, in_flight)
    }

    #[timed(miner_commit, labels(item = || item.as_ref()))]
    pub fn commit(&self, item: CommitItem, changes: State<Complete>) -> anyhow::Result<(), StorageError> {
        match item {
            CommitItem::Block(block) => self.commit_block(block, changes),
            CommitItem::ReplicationBlock(block) => {
                self.storage.set_pending_header(block.number(), block.timestamp());
                self.storage.finish_pending_block();
                let _in_flight = InFlightBlockGuard::new(Arc::clone(&self.storage), block.number());
                self.commit_block(block, changes)
            }
        }
    }

    /// Persists a mined block to permanent storage and prepares new block.
    pub fn commit_block(&self, block: Block, changes: State<Complete>) -> anyhow::Result<(), StorageError> {
        let block_number = block.number();

        // track
        #[cfg(feature = "tracing")]
        let _span = info_span!("miner::commit", %block_number).entered();
        tracing::info!(%block_number, transactions_len = %block.transactions.len(), "commiting block");

        // lock
        let _commit_lock = self.locks.commit.lock();

        tracing::info!(%block_number, "miner acquired commit lock");

        // extract fields to use in notifications if have subscribers
        let block_header = if self.has_block_header_subscribers() {
            Some(block.header.clone())
        } else {
            None
        };
        let block_logs = self.has_log_subscribers().then(|| block.create_log_messages());

        // save storage
        self.storage.save_block(block, changes)?;

        // Send notifications after saving the block
        self.send_log_notifications(&block_logs);
        self.send_block_header_notification(&block_header);

        Ok(())
    }

    // -----------------------------------------------------------------------------
    // Notification methods
    // -----------------------------------------------------------------------------

    /// Checks if there are any subscribers for block header notifications
    fn has_block_header_subscribers(&self) -> bool {
        self.notifier_blocks.receiver_count() > 0
    }

    /// Checks if there are any subscribers for log notifications
    fn has_log_subscribers(&self) -> bool {
        self.notifier_logs.receiver_count() > 0
    }

    /// Checks if there are any subscribers for pending transaction notifications
    fn has_pending_tx_subscribers(&self) -> bool {
        self.notifier_pending_txs.receiver_count() > 0
    }

    /// Sends a notification for a block header
    fn send_block_header_notification(&self, block_header: &Option<BlockHeader>) {
        if let Some(block_header) = block_header {
            let _ = self.notifier_blocks.send(block_header.clone());
        }
    }

    /// Sends notifications for logs
    fn send_log_notifications(&self, logs: &Option<Vec<LogMessage>>) {
        if let Some(logs) = logs {
            for log in logs {
                let _ = self.notifier_logs.send(log.clone());
            }
        }
    }

    /// Sends notifications for pending transactions
    fn send_pending_tx_notification(&self, tx_hash: &Option<Hash>) {
        if let Some(tx_hash) = tx_hash {
            let _ = self.notifier_pending_txs.send(*tx_hash);
        }
    }
}

// -----------------------------------------------------------------------------
// Miner
// -----------------------------------------------------------------------------
pub mod interval_miner {
    use std::sync::Arc;
    use std::sync::mpsc;
    use std::sync::mpsc::RecvTimeoutError;
    use std::time::Duration;

    use parking_lot::MutexGuard;
    use tokio::time::Instant;
    use tokio_util::sync::CancellationToken;

    use crate::eth::executor::State;
    use crate::eth::executor::types::state::Complete;
    use crate::eth::miner::Miner;
    use crate::eth::miner::miner::CommitItem;
    use crate::eth::types::Block;
    use crate::infra::tracing::warn_task_cancellation;
    use crate::infra::tracing::warn_task_rx_closed;

    pub fn run(miner: Arc<Miner>, ticks_rx: mpsc::Receiver<Instant>, cancellation: CancellationToken) {
        const TASK_NAME: &str = "interval-miner-ticker";

        loop {
            if cancellation.is_cancelled() {
                warn_task_cancellation(TASK_NAME);
                break;
            }

            let tick = match ticks_rx.recv_timeout(Duration::from_secs(2)) {
                Ok(tick) => tick,
                Err(RecvTimeoutError::Disconnected) => break,
                Err(RecvTimeoutError::Timeout) => {
                    tracing::warn!(timeout = "2s", "timeout reading miner channel, expected 1 block per second");
                    continue;
                }
            };

            if miner.is_paused() {
                tracing::warn!("skipping mining block because block mining is paused");
                continue;
            }

            // mine
            tracing::info!(lag_us = %tick.elapsed().as_micros(), "interval mining block");
            let miner_guard = miner.locks.mine_and_commit.lock();
            // the guard lives until commit_retry returns, which only happens once the block is saved
            let (block, changes, _in_flight) = miner.mine_local();
            commit_retry(&miner, block, changes, miner_guard);
        }
        warn_task_rx_closed(TASK_NAME);
    }

    pub fn commit_retry(miner: &Miner, block: Block, changes: State<Complete>, _miner_guard: MutexGuard<()>) {
        loop {
            match miner.commit(CommitItem::Block(block.clone()), changes.clone()) {
                Ok(_) => break,
                Err(e) => {
                    tracing::error!(reason = ?e, "failed to commit block");
                    continue;
                }
            }
        }
    }
}

mod interval_miner_ticker {
    use std::sync::mpsc;
    use std::thread;
    use std::time::Duration;

    use chrono::Timelike;
    use chrono::Utc;
    use tokio::time::Instant;
    use tokio_util::sync::CancellationToken;

    use crate::infra::tracing::warn_task_cancellation;
    use crate::infra::tracing::warn_task_rx_closed;

    pub async fn run(block_time: Duration, ticks_tx: mpsc::Sender<Instant>, cancellation: CancellationToken) {
        const TASK_NAME: &str = "interval-miner-ticker";

        // sync to next second
        #[allow(clippy::expect_used)]
        let next_second = (Utc::now() + Duration::from_secs(1))
            .with_nanosecond(0)
            .expect("nanosecond above is set to `0`, which is always less than 2 billion");

        let time_to_sleep = (next_second - Utc::now()).to_std().unwrap_or_default();
        thread::sleep(time_to_sleep);

        // prepare ticker
        let mut ticker = tokio::time::interval(block_time);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Burst);

        loop {
            if cancellation.is_cancelled() {
                warn_task_cancellation(TASK_NAME);
                return;
            }

            let tick = ticker.tick().await;
            if ticks_tx.send(tick).is_err() {
                warn_task_rx_closed(TASK_NAME);
                break;
            };
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::eth::executor::State;

    fn new_miner() -> (Arc<StratusStorage>, Miner) {
        let storage = Arc::new(StratusStorage::new_test().expect("failed to build test storage"));
        let miner = Miner::new(Arc::clone(&storage), MinerMode::External);
        (storage, miner)
    }

    #[test]
    fn mine_local_guard_keeps_transactions_until_saved() {
        let (storage, miner) = new_miner();
        let hash = Hash::new([0x11; 32]);
        storage.save_mock_execution_with_hash(hash);

        let (block, changes, in_flight) = miner.mine_local();
        assert!(storage.transaction_exists_recent(hash), "in-flight block");

        miner.commit(CommitItem::Block(block), changes).expect("commit");
        drop(in_flight);
        assert!(storage.transaction_exists_recent(hash), "committed block");
    }

    #[test]
    fn mine_local_guard_dropped_before_commit_discards_transactions() {
        let (storage, miner) = new_miner();
        let hash = Hash::new([0x22; 32]);
        storage.save_mock_execution_with_hash(hash);

        let (_block, _changes, in_flight) = miner.mine_local();
        drop(in_flight);
        assert!(!storage.transaction_exists_recent(hash));
    }

    /// External block matching the pending block's timestamp, with the given number.
    fn external_block(storage: &StratusStorage, number: crate::eth::types::BlockNumber) -> ExternalBlock {
        let mut block: ExternalBlock = crate::utils::test_utils::fake_first();
        block.0.header.inner.number = number.as_u64();
        block.0.header.inner.timestamp = **storage.read_pending_block_header().timestamp;
        block
    }

    #[test]
    fn mine_external_keeps_transactions_of_matching_block() {
        let (storage, miner) = new_miner();
        let hash = Hash::new([0x44; 32]);
        storage.save_mock_execution_with_hash(hash);
        let external = external_block(&storage, storage.read_pending_block_header().number);

        let (block, changes, in_flight) = miner.mine_external(external).expect("matching external block");
        miner.commit(CommitItem::Block(block), changes).expect("commit");
        drop(in_flight);

        assert!(storage.transaction_exists_recent(hash));
    }

    #[test]
    fn mine_external_mismatch_discards_transactions() {
        let (storage, miner) = new_miner();
        let hash = Hash::new([0x55; 32]);
        storage.save_mock_execution_with_hash(hash);
        let external = external_block(&storage, storage.read_pending_block_header().number.next_block_number());

        let error = miner.mine_external(external).err().expect("mismatching external block must fail");
        assert!(error.to_string().contains("mismatching block info"), "unexpected error: {error}");

        assert!(!storage.transaction_exists_recent(hash), "dropped block must not be reported");
    }

    #[test]
    fn concurrent_reader_never_misses_a_transaction_being_mined() {
        use std::sync::atomic::Ordering;

        let (storage, miner) = new_miner();
        for round in 0..300u32 {
            let mut bytes = [0xA0; 32];
            bytes[..4].copy_from_slice(&round.to_be_bytes());
            let hash = Hash::new(bytes);
            storage.save_mock_execution_with_hash(hash);

            let started = AtomicBool::new(false);
            let done = AtomicBool::new(false);
            let misses = std::thread::scope(|scope| {
                let reader = scope.spawn(|| {
                    let mut misses = 0u64;
                    loop {
                        let finished = done.load(Ordering::Acquire);
                        if !storage.transaction_exists_recent(hash) {
                            misses += 1;
                        }
                        started.store(true, Ordering::Release);
                        if finished {
                            return misses;
                        }
                    }
                });

                // only mine once the reader is spinning, so finish and commit race against it
                while !started.load(Ordering::Acquire) {
                    std::hint::spin_loop();
                }
                let (block, changes, _in_flight) = miner.mine_local();
                miner.commit(CommitItem::Block(block), changes).expect("commit");
                done.store(true, Ordering::Release);

                reader.join().expect("reader thread")
            });

            assert_eq!(
                misses, 0,
                "round {round}: transaction reported missing while moving pending -> in flight -> committed"
            );
        }
    }

    #[test]
    fn dropping_an_old_guard_keeps_the_next_in_flight_block() {
        let (storage, miner) = new_miner();
        let first = Hash::new([0x66; 32]);
        let second = Hash::new([0x67; 32]);

        storage.save_mock_execution_with_hash(first);
        let (block, changes, first_guard) = miner.mine_local();
        miner.commit(CommitItem::Block(block), changes).expect("commit");

        storage.save_mock_execution_with_hash(second);
        let (_block, _changes, _second_guard) = miner.mine_local();

        drop(first_guard);
        assert!(storage.transaction_exists_recent(first), "committed block");
        assert!(
            storage.transaction_exists_recent(second),
            "next in-flight block must not be discarded by an older guard"
        );
    }

    #[test]
    fn failed_replication_commit_does_not_index_transactions() {
        let (leader, leader_miner) = new_miner();
        let hash = Hash::new([0x34; 32]);
        leader.save_mock_execution_with_hash(hash);
        let (mut block, _, _in_flight) = leader_miner.mine_local();

        // a block that skips a number fails the mined number check
        block.header.number = block.header.number.next_block_number();
        let (follower, follower_miner) = new_miner();
        follower_miner
            .commit(CommitItem::ReplicationBlock(block), State::default())
            .expect_err("replicated block with a gap must fail");

        assert!(!follower.transaction_exists_recent(hash));
    }

    #[test]
    fn replication_commit_indexes_replicated_transactions() {
        // the leader mines a block with a transaction
        let (leader, leader_miner) = new_miner();
        let hash = Hash::new([0x33; 32]);
        leader.save_mock_execution_with_hash(hash);
        let (block, _, _in_flight) = leader_miner.mine_local();

        // a replication follower commits it without executing it
        let (follower, follower_miner) = new_miner();
        follower_miner
            .commit(CommitItem::ReplicationBlock(block), State::default())
            .expect("commit replicated block");

        assert!(follower.transaction_exists_recent(hash));
    }
}
