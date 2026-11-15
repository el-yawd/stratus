//! Transaction hashes of the block being committed and of the most recently committed blocks.

use std::collections::VecDeque;

use rustc_hash::FxHashMap;
use rustc_hash::FxHashSet;

use crate::eth::types::BlockNumber;
use crate::eth::types::Hash;

/// Default number of most recent mined blocks whose transaction hashes are kept in memory.
pub const DEFAULT_RECENT_TRANSACTIONS_BLOCKS: usize = 5;

/// Maximum configurable window size. The window is reloaded block by block from the permanent storage on startup.
pub const MAX_RECENT_TRANSACTIONS_BLOCKS: usize = 1000;

/// Exact index of the transaction hashes of:
/// - the in-flight block: finished (no longer pending) but not yet saved to the permanent storage;
/// - the committed window: the last `capacity_blocks` blocks successfully saved to the permanent storage.
///
/// A block only enters the committed window after it is saved. The in-flight block is moved to the committed window
/// when it is saved, or discarded if saving fails or the block is dropped, so hashes of a block that was never saved
/// are never reported.
///
/// Hashes are mapped to the most recent block that contained them, so a hash that appears in more than one
/// block in the window is only removed when its most recent block is evicted.
#[derive(Debug)]
pub struct RecentTransactions {
    capacity_blocks: usize,
    in_flight: Option<(BlockNumber, FxHashSet<Hash>)>,
    blocks: VecDeque<(BlockNumber, Vec<Hash>)>,
    hashes: FxHashMap<Hash, BlockNumber>,
}

impl RecentTransactions {
    pub fn new(capacity_blocks: usize) -> Self {
        Self {
            capacity_blocks,
            in_flight: None,
            blocks: VecDeque::with_capacity(capacity_blocks + 1),
            hashes: FxHashMap::default(),
        }
    }

    pub fn capacity_blocks(&self) -> usize {
        self.capacity_blocks
    }

    /// Marks a finished block as in flight.
    ///
    /// Blocks are mined and committed one at a time (miner locks and sequential importers), so the previous in-flight
    /// block must already be committed or discarded. If it is not, it is replaced and its transactions are no longer
    /// reported until it is committed: this breaks the invariant, so it is logged as an error.
    pub fn set_in_flight(&mut self, number: BlockNumber, hashes: FxHashSet<Hash>) {
        if let Some((previous, _)) = &self.in_flight
            && *previous != number
        {
            tracing::error!(%previous, new = %number, "finished a block while another block was still in flight");
        }
        self.in_flight = Some((number, hashes));
    }

    /// Discards the in-flight block if it has the given number (its commit failed or it was dropped).
    pub fn discard_in_flight(&mut self, number: BlockNumber) {
        if self.in_flight.as_ref().is_some_and(|(in_flight_number, _)| *in_flight_number == number) {
            self.in_flight = None;
        }
    }

    /// Adds a block saved to the permanent storage to the committed window, and clears the in-flight block if it
    /// has the same number. Both happen under the same lock, so a reader never misses the block in between.
    pub fn commit_block(&mut self, number: BlockNumber, hashes: impl IntoIterator<Item = Hash>) {
        self.push_block(number, hashes);
        self.discard_in_flight(number);
    }

    /// Adds the transaction hashes of a committed block, evicting the oldest blocks beyond the window capacity.
    ///
    /// Pushing the same block number as the newest block in the window merges the hashes into it instead of
    /// taking a new slot, so indexing a block twice does not evict other blocks early.
    fn push_block(&mut self, number: BlockNumber, hashes: impl IntoIterator<Item = Hash>) {
        let hashes: Vec<Hash> = hashes.into_iter().collect();
        for hash in &hashes {
            self.hashes.insert(*hash, number);
        }
        match self.blocks.back_mut() {
            Some((newest_number, newest_hashes)) if *newest_number == number => newest_hashes.extend(hashes),
            _ => self.blocks.push_back((number, hashes)),
        }

        while self.blocks.len() > self.capacity_blocks {
            let Some((evicted_number, evicted_hashes)) = self.blocks.pop_front() else {
                break;
            };
            for hash in evicted_hashes {
                // only remove if no newer block in the window also contains this hash
                if self.hashes.get(&hash) == Some(&evicted_number) {
                    self.hashes.remove(&hash);
                }
            }
        }
    }

    pub fn contains(&self, hash: &Hash) -> bool {
        self.in_flight.as_ref().is_some_and(|(_, hashes)| hashes.contains(hash)) || self.hashes.contains_key(hash)
    }

    pub fn clear(&mut self) {
        self.in_flight = None;
        self.blocks.clear();
        self.hashes.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hash(n: u8) -> Hash {
        Hash::new([n; 32])
    }

    fn set<const N: usize>(hashes: [Hash; N]) -> FxHashSet<Hash> {
        hashes.into_iter().collect()
    }

    #[test]
    fn contains_pushed_hashes() {
        let mut recent = RecentTransactions::new(2);
        recent.commit_block(1.into(), [hash(1), hash(2)]);
        assert!(recent.contains(&hash(1)));
        assert!(recent.contains(&hash(2)));
        assert!(!recent.contains(&hash(3)));
    }

    #[test]
    fn evicts_oldest_block_beyond_capacity() {
        let mut recent = RecentTransactions::new(2);
        recent.commit_block(1.into(), [hash(1)]);
        recent.commit_block(2.into(), [hash(2)]);
        recent.commit_block(3.into(), [hash(3)]);
        assert!(!recent.contains(&hash(1)));
        assert!(recent.contains(&hash(2)));
        assert!(recent.contains(&hash(3)));
    }

    #[test]
    fn duplicated_hash_survives_eviction_of_older_block() {
        let mut recent = RecentTransactions::new(2);
        recent.commit_block(1.into(), [hash(1)]);
        recent.commit_block(2.into(), [hash(1)]);
        recent.commit_block(3.into(), []);
        assert!(recent.contains(&hash(1)), "hash is still in block 2");
        recent.commit_block(4.into(), []);
        assert!(!recent.contains(&hash(1)));
    }

    #[test]
    fn hash_in_evicted_and_new_block_is_kept() {
        let mut recent = RecentTransactions::new(1);
        recent.commit_block(1.into(), [hash(1)]);
        recent.commit_block(2.into(), [hash(1)]);
        assert!(recent.contains(&hash(1)), "evicted from block 1 but re-inserted for block 2");
        recent.commit_block(3.into(), []);
        assert!(!recent.contains(&hash(1)));
    }

    #[test]
    fn zero_capacity_stores_nothing() {
        let mut recent = RecentTransactions::new(0);
        recent.commit_block(1.into(), [hash(1)]);
        assert!(!recent.contains(&hash(1)));
    }

    #[test]
    fn same_block_number_is_merged() {
        let mut recent = RecentTransactions::new(2);
        recent.commit_block(1.into(), [hash(1)]);
        recent.commit_block(2.into(), []);
        recent.commit_block(2.into(), [hash(2)]);
        recent.commit_block(2.into(), [hash(2)]);
        assert!(recent.contains(&hash(1)), "block 1 must not be evicted by repeated pushes of block 2");
        assert!(recent.contains(&hash(2)));
        recent.commit_block(3.into(), []);
        recent.commit_block(4.into(), []);
        assert!(!recent.contains(&hash(2)));
    }

    #[test]
    fn in_flight_is_visible_until_committed() {
        let mut recent = RecentTransactions::new(1);
        recent.set_in_flight(1.into(), set([hash(1)]));
        assert!(recent.contains(&hash(1)));
        recent.commit_block(1.into(), [hash(1)]);
        assert!(recent.contains(&hash(1)), "moved to the committed window");
        recent.commit_block(2.into(), []);
        assert!(!recent.contains(&hash(1)), "in-flight entry must not outlive the commit");
    }

    #[test]
    fn discarded_in_flight_is_not_visible() {
        let mut recent = RecentTransactions::new(2);
        recent.set_in_flight(1.into(), set([hash(1)]));
        recent.discard_in_flight(1.into());
        assert!(!recent.contains(&hash(1)));
    }

    #[test]
    fn discard_and_commit_only_affect_the_matching_in_flight_block() {
        let mut recent = RecentTransactions::new(2);
        recent.set_in_flight(2.into(), set([hash(2)]));
        recent.discard_in_flight(1.into());
        recent.commit_block(1.into(), [hash(1)]);
        assert!(recent.contains(&hash(2)), "in-flight block 2 is untouched by block 1");
    }

    #[test]
    fn set_in_flight_replaces_previous_in_flight() {
        let mut recent = RecentTransactions::new(2);
        recent.set_in_flight(1.into(), set([hash(1)]));
        recent.set_in_flight(2.into(), set([hash(2)]));
        assert!(!recent.contains(&hash(1)));
        assert!(recent.contains(&hash(2)));
    }

    #[test]
    fn zero_capacity_only_sees_in_flight() {
        let mut recent = RecentTransactions::new(0);
        recent.set_in_flight(1.into(), set([hash(1)]));
        assert!(recent.contains(&hash(1)));
        recent.commit_block(1.into(), [hash(1)]);
        assert!(!recent.contains(&hash(1)));
    }

    #[test]
    fn clear_removes_everything() {
        let mut recent = RecentTransactions::new(2);
        recent.commit_block(1.into(), [hash(1)]);
        recent.set_in_flight(2.into(), set([hash(2)]));
        recent.clear();
        assert!(!recent.contains(&hash(2)));
        assert!(!recent.contains(&hash(1)));
    }
}
