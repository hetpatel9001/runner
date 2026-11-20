use super::VolatileStorage;
use raft::{GetEntriesContext, Storage, StorageError, prelude::*};

#[test]
fn applied_history_is_compacted_without_forgetting_the_current_vote() -> raft::Result<()> {
    let storage = VolatileStorage::new();
    let conf = ConfState::from((vec![11, 22, 33], vec![]));
    let mut hard = HardState::default();
    hard.term = 7;
    hard.vote = 22;
    for index in 1..=1024 {
        let mut entry = Entry::default();
        entry.index = index;
        entry.term = hard.term;
        entry.data = vec![42; 1024].into();
        storage.append(&[entry])?;
        hard.commit = index;
        storage.hard_state(hard.clone());
        storage.compact(index, &conf, index.to_le_bytes().to_vec())?;

        assert_eq!(storage.first_index()?, index + 1);
        assert_eq!(storage.last_index()?, index);
        assert_eq!(storage.term(index)?, hard.term);
        assert!(matches!(
            storage.entries(index, index + 1, None, GetEntriesContext::empty(false)),
            Err(raft::Error::Store(StorageError::Compacted))
        ));
    }
    let snapshot = storage.snapshot(1024, 0)?;
    assert_eq!(snapshot.get_metadata().index, 1024);
    assert_eq!(snapshot.get_metadata().get_conf_state(), &conf);
    assert_eq!(snapshot.data.as_ref(), 1024_u64.to_le_bytes());
    assert_eq!(storage.initial_state()?.hard_state, hard);
    Ok(())
}

#[test]
fn a_new_storage_has_no_previous_term_vote_membership_or_log() -> raft::Result<()> {
    let previous = VolatileStorage::new();
    let mut hard = HardState::default();
    hard.term = 9000;
    hard.vote = 22;
    previous.hard_state(hard);
    let mut snapshot = Snapshot::default();
    snapshot.mut_metadata().index = 50;
    snapshot.mut_metadata().term = 9000;
    snapshot
        .mut_metadata()
        .set_conf_state(ConfState::from((vec![11, 22, 33], vec![])));
    snapshot.data = b"previous incarnation".to_vec().into();
    previous.apply_snapshot(snapshot)?;
    drop(previous);

    let fresh = VolatileStorage::new();
    let state = fresh.initial_state()?;
    assert_eq!(state.hard_state, HardState::default());
    assert_eq!(state.conf_state, ConfState::default());
    assert_eq!(fresh.first_index()?, 1);
    assert_eq!(fresh.last_index()?, 0);
    assert_eq!(fresh.snapshot(0, 0)?, Snapshot::default());
    Ok(())
}
