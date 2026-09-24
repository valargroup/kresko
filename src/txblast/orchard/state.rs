use std::collections::{HashMap, HashSet};

use incrementalmerkletree::Position;

use super::types::{NoteRole, RecoveredNote, TrackedNote, TreasuryUtxo};

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct RegistrySnapshot {
    pub(crate) ready_lanes: usize,
    pub(crate) reservoirs: usize,
    pub(crate) drained_notes: u64,
    pub(crate) lane_total_value: u64,
    pub(crate) reservoir_total_value: u64,
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct TreasurySnapshot {
    pub(crate) backlog_utxos: usize,
    pub(crate) backlog_value: u64,
    pub(crate) reserved_utxos: usize,
}

#[derive(Default)]
pub(crate) struct LaneRegistry {
    next_lane_id: u64,
    retry_height: Option<u32>,
    deferred_notes: HashSet<String>,
    ready_lanes: Vec<TrackedNote>,
    reservoir_notes: Vec<TrackedNote>,
    drained_notes: u64,
}

impl LaneRegistry {
    pub(crate) fn ready_lane_count(&self) -> usize {
        self.ready_lanes.len()
    }

    pub(crate) fn reservoir_count(&self) -> usize {
        self.reservoir_notes.len()
    }

    pub(crate) fn spendable_note_count(&self) -> usize {
        self.ready_lane_count() + self.reservoir_count()
    }

    pub(crate) fn spendable_notes(&self) -> Vec<TrackedNote> {
        self.ready_lanes
            .iter()
            .chain(self.reservoir_notes.iter())
            .cloned()
            .collect()
    }

    pub(crate) fn spendable_value(&self) -> u64 {
        self.ready_lanes
            .iter()
            .chain(self.reservoir_notes.iter())
            .map(TrackedNote::value)
            .sum()
    }

    pub(crate) fn drained_notes(&self) -> u64 {
        self.drained_notes
    }

    pub(crate) fn snapshot(&self) -> RegistrySnapshot {
        RegistrySnapshot {
            ready_lanes: self.ready_lane_count(),
            reservoirs: self.reservoir_count(),
            drained_notes: self.drained_notes,
            lane_total_value: self.ready_lanes.iter().map(TrackedNote::value).sum(),
            reservoir_total_value: self.reservoir_notes.iter().map(TrackedNote::value).sum(),
        }
    }

    pub(crate) fn reset_for_rebuild(&mut self) {
        self.next_lane_id = 0;
        self.deferred_notes.clear();
        self.retry_height = None;
        self.ready_lanes.clear();
        self.reservoir_notes.clear();
    }

    pub(crate) fn activate_recovered_note(
        &mut self,
        recovered: RecoveredNote,
        position: Position,
        confirmation_height: u32,
    ) -> TrackedNote {
        let lane_id = match recovered.role {
            NoteRole::Lane => {
                let lane_id = self.next_lane_id;
                self.next_lane_id += 1;
                Some(lane_id)
            }
            NoteRole::Reservoir => None,
        };

        let tracked = TrackedNote {
            note_id: recovered.note_id,
            parent_note_id: recovered.parent_note_id,
            origin_txid: recovered.origin_txid,
            origin_action_idx: recovered.action_idx,
            lane_id,
            note: recovered.note,
            position,
            role: recovered.role,
            last_confirmation_height: confirmation_height,
        };

        self.requeue(tracked.clone());
        tracked
    }

    pub(crate) fn requeue(&mut self, tracked: TrackedNote) {
        match tracked.role {
            NoteRole::Lane => self.ready_lanes.push(tracked),
            NoteRole::Reservoir => self.reservoir_notes.push(tracked),
        }
    }

    pub(crate) fn promote_reservoir_to_lane(&mut self, mut tracked: TrackedNote) -> TrackedNote {
        self.assign_lane_metadata(&mut tracked);
        self.ready_lanes.push(tracked.clone());
        tracked
    }

    pub(crate) fn retry_at_height(&mut self, height: u32) {
        if self.retry_height != Some(height) {
            self.deferred_notes.clear();
            self.retry_height = Some(height);
        }
    }

    pub(crate) fn defer_failed_lane(&mut self, tracked: TrackedNote) {
        self.deferred_notes.insert(tracked.note_id.clone());
        self.requeue(tracked);
    }

    pub(crate) fn take_ready_lane(&mut self) -> Option<TrackedNote> {
        if let Some(tracked) = pop_available(&mut self.ready_lanes, &self.deferred_notes) {
            return Some(tracked);
        }

        let mut tracked = pop_available(&mut self.reservoir_notes, &self.deferred_notes)?;
        self.assign_lane_metadata(&mut tracked);
        Some(tracked)
    }

    pub(crate) fn take_reservoir(&mut self) -> Option<TrackedNote> {
        pop_highest_value(&mut self.reservoir_notes)
    }

    pub(crate) fn remove_note(&mut self, note_id: &str) -> Option<TrackedNote> {
        remove_note_by_id(&mut self.ready_lanes, note_id)
            .or_else(|| remove_note_by_id(&mut self.reservoir_notes, note_id))
    }

    pub(crate) fn drain_note(&mut self) {
        self.drained_notes += 1;
    }

    fn assign_lane_metadata(&mut self, tracked: &mut TrackedNote) {
        let lane_id = self.next_lane_id;
        self.next_lane_id += 1;
        tracked.role = NoteRole::Lane;
        tracked.lane_id = Some(lane_id);
    }
}

fn pop_available(notes: &mut Vec<TrackedNote>, deferred: &HashSet<String>) -> Option<TrackedNote> {
    let (idx, _) = notes
        .iter()
        .enumerate()
        .filter(|(_, note)| !deferred.contains(&note.note_id))
        .max_by_key(|(_, note)| note.value())?;
    Some(notes.swap_remove(idx))
}

fn pop_highest_value(notes: &mut Vec<TrackedNote>) -> Option<TrackedNote> {
    let (idx, _) = notes
        .iter()
        .enumerate()
        .max_by_key(|(_, tracked)| tracked.value())?;
    Some(notes.swap_remove(idx))
}

fn remove_note_by_id(notes: &mut Vec<TrackedNote>, note_id: &str) -> Option<TrackedNote> {
    let idx = notes
        .iter()
        .position(|tracked| tracked.note_id == note_id)?;
    Some(notes.swap_remove(idx))
}

#[derive(Default)]
pub(crate) struct TreasuryInventory {
    ready_utxos: Vec<TreasuryUtxo>,
    reserved_utxos: HashMap<String, TreasuryUtxo>,
    consumed_outpoints: HashSet<String>,
}

impl TreasuryInventory {
    pub(crate) fn backlog_count(&self) -> usize {
        self.ready_utxos.len()
    }

    pub(crate) fn backlog_value(&self) -> u64 {
        self.ready_utxos.iter().map(|utxo| utxo.satoshis).sum()
    }

    pub(crate) fn snapshot(&self) -> TreasurySnapshot {
        TreasurySnapshot {
            backlog_utxos: self.backlog_count(),
            backlog_value: self.backlog_value(),
            reserved_utxos: self.reserved_utxos.len(),
        }
    }

    pub(crate) fn refresh_discovered(&mut self, discovered: Vec<TreasuryUtxo>) {
        let discovered_ids = discovered
            .iter()
            .map(|utxo| utxo.outpoint_id.clone())
            .collect::<HashSet<_>>();
        self.ready_utxos
            .retain(|utxo| discovered_ids.contains(&utxo.outpoint_id));

        let mut ready_ids = self
            .ready_utxos
            .iter()
            .map(|utxo| utxo.outpoint_id.clone())
            .collect::<HashSet<_>>();

        for utxo in discovered {
            if ready_ids.contains(&utxo.outpoint_id)
                || self.reserved_utxos.contains_key(&utxo.outpoint_id)
                || self.consumed_outpoints.contains(&utxo.outpoint_id)
            {
                continue;
            }

            ready_ids.insert(utxo.outpoint_id.clone());
            self.ready_utxos.push(utxo);
        }
    }

    pub(crate) fn take_ready_utxo(&mut self) -> Option<TreasuryUtxo> {
        let utxo = pop_highest_value_by(&mut self.ready_utxos, |utxo| utxo.satoshis)?;
        self.reserved_utxos
            .insert(utxo.outpoint_id.clone(), utxo.clone());
        Some(utxo)
    }

    pub(crate) fn confirm_spent(&mut self, outpoint_id: &str) {
        self.reserved_utxos.remove(outpoint_id);
        self.ready_utxos
            .retain(|utxo| utxo.outpoint_id != outpoint_id);
        self.consumed_outpoints.insert(outpoint_id.to_owned());
    }

    pub(crate) fn requeue(&mut self, utxo: TreasuryUtxo) {
        self.reserved_utxos.remove(&utxo.outpoint_id);
        if self
            .ready_utxos
            .iter()
            .all(|ready| ready.outpoint_id != utxo.outpoint_id)
        {
            self.ready_utxos.push(utxo);
        }
    }
}

fn pop_highest_value_by<T, F>(items: &mut Vec<T>, value: F) -> Option<T>
where
    F: Fn(&T) -> u64,
{
    let (idx, _) = items
        .iter()
        .enumerate()
        .max_by_key(|(_, item)| value(item))?;
    Some(items.swap_remove(idx))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn treasury(id: &str, satoshis: u64) -> TreasuryUtxo {
        TreasuryUtxo {
            outpoint_id: id.to_owned(),
            txid: "txid".to_owned(),
            output_index: 0,
            script: "51".to_owned(),
            satoshis,
            height: 1,
        }
    }

    #[test]
    fn treasury_refresh_preserves_reserved_and_deduplicates() {
        let mut inventory = TreasuryInventory::default();
        inventory.refresh_discovered(vec![treasury("a:0", 100), treasury("b:0", 200)]);
        let reserved = inventory.take_ready_utxo().expect("reserved utxo");
        assert_eq!(reserved.outpoint_id, "b:0");

        inventory.refresh_discovered(vec![treasury("a:0", 100), treasury("b:0", 200)]);

        assert_eq!(inventory.backlog_count(), 1);
        assert_eq!(inventory.backlog_value(), 100);
        assert_eq!(inventory.snapshot().reserved_utxos, 1);
    }

    #[test]
    fn treasury_confirm_removes_ready_and_reserved_entries() {
        let mut inventory = TreasuryInventory::default();
        inventory.refresh_discovered(vec![treasury("a:0", 100), treasury("b:0", 200)]);
        let reserved = inventory.take_ready_utxo().expect("reserved utxo");
        inventory.confirm_spent(&reserved.outpoint_id);

        assert_eq!(inventory.backlog_count(), 1);
        assert_eq!(inventory.snapshot().reserved_utxos, 0);

        inventory.refresh_discovered(vec![treasury("a:0", 100), treasury("b:0", 200)]);
        assert_eq!(inventory.backlog_count(), 1);
    }
}

#[cfg(test)]
mod lane_retry_tests {
    use super::*;
    use orchard::{
        keys::{FullViewingKey, Scope, SpendingKey},
        note::{RandomSeed, Rho},
        value::NoteValue,
    };
    fn test_tracked_note(value_zats: u64, idx: usize) -> TrackedNote {
        let sk = SpendingKey::from_bytes([7u8; 32]).unwrap();
        let fvk = FullViewingKey::from(&sk);
        let address = fvk.address_at(0u32, Scope::External);
        let mut rho_bytes = [0u8; 32];
        rho_bytes[0] = (idx as u8).wrapping_add(1);
        let rho = Rho::from_bytes(&rho_bytes).unwrap();
        let rseed = (1u8..=u8::MAX)
            .find_map(|byte| {
                let mut rseed_bytes = [0u8; 32];
                rseed_bytes[0] = byte;
                Option::<RandomSeed>::from(RandomSeed::from_bytes(rseed_bytes, &rho))
            })
            .expect("valid random seed");
        let note = Option::<orchard::Note>::from(orchard::Note::from_parts(
            address,
            NoteValue::from_raw(value_zats),
            rho,
            rseed,
            // V2 is the ZIP 212 Orchard plaintext. V3 is the Ironwood
            // (ZIP 2005) format -- switching pools is a deliberate change,
            // not a default.
            orchard::note::NoteVersion::V2,
        ))
        .expect("valid test note");

        TrackedNote {
            note_id: format!("note-{idx}"),
            parent_note_id: None,
            origin_txid: format!("txid-{idx}"),
            origin_action_idx: idx,
            lane_id: None,
            note,
            position: Position::from(idx as u64),
            role: NoteRole::Reservoir,
            last_confirmation_height: 1,
        }
    }
    #[test]
    fn conflicting_high_value_lane_does_not_starve_other_lanes() {
        let mut registry = LaneRegistry::default();
        registry.requeue(test_tracked_note(90_000, 0));
        registry.requeue(test_tracked_note(30_000, 1));
        registry.retry_at_height(600);
        let rejected = registry.take_ready_lane().unwrap();
        assert_eq!(rejected.note_id, "note-0");
        registry.defer_failed_lane(rejected);
        assert_eq!(registry.take_ready_lane().unwrap().note_id, "note-1");
        registry.retry_at_height(600);
        assert!(registry.take_ready_lane().is_none());
        registry.retry_at_height(601);
        assert_eq!(registry.take_ready_lane().unwrap().note_id, "note-0");
    }
}
