use crate::{config::Config, model::*, storage::VolatileStorage};
use anyhow::{bail, ensure};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use bytes::Bytes;
use protobuf::Message as _;
use raft::{RawNode, SnapshotStatus, StateRole, prelude::*};
use runner_core::{bounded::ByteQueue, random_id};
use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::{mpsc, oneshot, watch};
use tokio_util::sync::CancellationToken;

pub(crate) type Reply<T> = oneshot::Sender<anyhow::Result<T>>;
pub(crate) enum Event {
    Bootstrap(Manifest, Reply<()>),
    Join(Identity, Reply<Registry>),
    Adopt(Registry),
    Message(
        Message,
        Option<Identity>,
        Option<tokio::sync::OwnedSemaphorePermit>,
    ),
    Verify(Reply<Arc<View>>),
    Transfer(u64),
    Fence(u64, Reply<()>),
    Pull {
        node: u64,
        boot: String,
        generation: String,
        reply: Reply<Vec<Envelope>>,
    },
    Delivered {
        to: u64,
        sequence: u64,
        snapshot: bool,
        ok: bool,
    },
}
#[derive(Clone)]
pub(crate) struct Packet {
    pub sequence: u64,
    pub to: u64,
    pub snapshot: bool,
    pub generation: Arc<str>,
    pub data: Bytes,
}
struct Pending {
    since: Instant,
    replies: Vec<Reply<Arc<View>>>,
}

pub(crate) struct Core {
    config: Arc<Config>,
    identity: Identity,
    state: Registry,
    conf: ConfState,
    node: Option<RawNode<VolatileStorage>>,
    storage: VolatileStorage,
    applied: u64,
    retired: bool,
    conf_pending: bool,
    pending: BTreeMap<String, Pending>,
    verify_queue: Vec<Reply<Arc<View>>>,
    learner_seen: BTreeMap<u64, Instant>,
    mailbox: BTreeMap<u64, ByteQueue<Arc<Packet>>>,
    // Authenticated routes for leaders whose committed membership snapshot
    // has not reached us yet. One route per configured logical slot, no votes.
    transports: BTreeMap<u64, Identity>,
    sequence: u64,
    minimum_term: u64,
    outgoing: BTreeMap<u64, mpsc::Sender<Arc<Packet>>>,
    status: watch::Sender<Arc<View>>,
}
impl Core {
    pub fn new(
        config: Arc<Config>,
        identity: Identity,
        status: watch::Sender<Arc<View>>,
        outgoing: BTreeMap<u64, mpsc::Sender<Arc<Packet>>>,
    ) -> Self {
        Self {
            config,
            identity,
            status,
            outgoing,
            state: Registry::default(),
            conf: ConfState::default(),
            node: None,
            storage: VolatileStorage::new(),
            applied: 0,
            retired: false,
            conf_pending: false,
            pending: BTreeMap::new(),
            verify_queue: Vec::new(),
            learner_seen: BTreeMap::new(),
            mailbox: BTreeMap::new(),
            transports: BTreeMap::new(),
            sequence: 0,
            minimum_term: 0,
        }
    }
    fn view(&self) -> Arc<View> {
        let mut view = View {
            id: self.identity.id,
            node: self.identity.node,
            boot: self.identity.boot.clone(),
            members: self.config.members.len(),
            quorum: self.config.members.len() / 2 + 1,
            phase: "uninitialized".into(),
            role: "Uninitialized".into(),
            generation: self.state.generation.clone(),
            registry: self.state.clone(),
            ..View::default()
        };
        if let Some(node) = &self.node {
            let state = &node.raft;
            view.leader_node = state.leader_id;
            view.leader = self.state.nodes.get(&state.leader_id).map_or(0, |n| n.id);
            view.term = state.term;
            view.role = match state.state {
                StateRole::Leader => "StateLeader",
                StateRole::Follower => "StateFollower",
                StateRole::Candidate => "StateCandidate",
                StateRole::PreCandidate => "StatePreCandidate",
            }
            .into();
            view.applied = self.applied;
            view.voters = self.conf.voters.clone();
            view.outgoing = self.conf.voters_outgoing.clone();
            view.learners = self.conf.learners.clone();
            view.identity_valid = voter(&self.conf, self.identity.node)
                && self.state.slots.get(&self.identity.id) == Some(&self.identity.node)
                && !self.retired;
            view.phase = if self.retired {
                "retired"
            } else if view.identity_valid {
                "voter"
            } else {
                "joining"
            }
            .into();
        }
        Arc::new(view)
    }
    fn initialize(&mut self, state: Registry, seed: Option<&Manifest>) -> anyhow::Result<()> {
        ensure!(self.node.is_none(), "process already has cluster state");
        self.state = state;
        if let Some(seed) = seed {
            self.conf.voters = seed.members.iter().map(|m| m.node).collect();
            self.conf.voters.sort_unstable();
            self.applied = 1;
            let mut snapshot = Snapshot::default();
            snapshot.data = serde_json::to_vec(&self.state)?.into();
            snapshot.mut_metadata().index = 1;
            snapshot.mut_metadata().term = seed.term;
            snapshot.mut_metadata().set_conf_state(self.conf.clone());
            self.storage.apply_snapshot(snapshot)?;
            let mut hard = HardState::default();
            hard.term = seed.term;
            hard.commit = 1;
            self.storage.hard_state(hard);
        }
        let config = raft::Config {
            id: self.identity.node,
            election_tick: self.config.election_ticks,
            heartbeat_tick: self.config.heartbeat_ticks,
            applied: self.applied,
            max_size_per_msg: 64 << 10,
            max_inflight_msgs: 16,
            check_quorum: true,
            pre_vote: true,
            ..raft::Config::default()
        };
        // Consensus state is observable through /status. Keep routine heartbeat
        // logging out of allocations and disk writes on every replica.
        let logger = slog::Logger::root(slog::Discard, slog::o!());
        self.node = Some(RawNode::new(&config, self.storage.clone(), &logger)?);
        if self.minimum_term > self.node.as_ref().unwrap().raft.term {
            self.node
                .as_mut()
                .unwrap()
                .raft
                .become_follower(self.minimum_term, 0);
        }
        Ok(())
    }
    fn bootstrap(&mut self, manifest: Manifest) -> anyhow::Result<()> {
        ensure!(
            !manifest.generation.is_empty()
                && (1..=1 << 52).contains(&manifest.term)
                && manifest.members.len() == self.config.members.len(),
            "invalid bootstrap manifest"
        );
        if !self.state.generation.is_empty() {
            ensure!(
                self.state.generation == manifest.generation,
                "refusing to overwrite an initialized cluster"
            );
            return Ok(());
        }
        let mut state = Registry {
            generation: manifest.generation.clone(),
            ..Registry::default()
        };
        for member in &manifest.members {
            ensure!(
                (1..=1 << 52).contains(&member.node)
                    && !member.boot.is_empty()
                    && self.config.peers.contains_key(&member.id)
                    && !state.slots.contains_key(&member.id)
                    && !state.nodes.contains_key(&member.node),
                "invalid bootstrap identities"
            );
            ensure!(
                member.id != self.identity.id || *member == self.identity,
                "bootstrap targets another incarnation"
            );
            state.slots.insert(member.id, member.node);
            state.nodes.insert(member.node, member.clone());
        }
        ensure!(
            state.slots.get(&self.identity.id) == Some(&self.identity.node),
            "bootstrap omits this incarnation"
        );
        self.initialize(state, Some(&manifest))
    }
    fn propose(&mut self, mut change: ConfChangeV2, context: Change) -> anyhow::Result<()> {
        ensure!(
            !self.conf_pending && self.conf.voters_outgoing.is_empty(),
            "membership change is in progress"
        );
        change.context = serde_json::to_vec(&context)?.into();
        self.node
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("not initialized"))?
            .propose_conf_change(Vec::new(), change)?;
        self.conf_pending = true;
        Ok(())
    }
    fn join(&mut self, member: Identity) -> anyhow::Result<Registry> {
        ensure!(
            !self.retired
                && self
                    .node
                    .as_ref()
                    .is_some_and(|n| n.raft.leader_id == self.identity.node),
            "not the current leader"
        );
        ensure!(
            (1..=1 << 52).contains(&member.node)
                && member.boot.len() >= 16
                && member.boot.len() <= 128
                && self.config.peers.contains_key(&member.id),
            "invalid joining incarnation"
        );
        ensure!(
            !self.state.retired.contains(&member.node),
            "retired incarnation; restart the process before rejoining"
        );
        if let Some(before) = self.state.nodes.get(&member.node) {
            ensure!(*before == member, "Raft identity collision");
            return Ok(self.state.clone());
        }
        // Bound revocation memory. Never forget an old identity merely to
        // admit another process; an explicit fresh bootstrap clears it.
        ensure!(
            self.state.retired.len() < 4096,
            "generation admission limit reached"
        );
        ensure!(
            self.conf.learners.is_empty() && self.conf.learners_next.is_empty(),
            "another replacement is joining"
        );
        let change = changes(&[(ConfChangeType::AddLearnerNode, member.node)], false);
        self.propose(
            change,
            Change {
                kind: "learner".into(),
                previous: self.state.slots.get(&member.id).copied().unwrap_or(0),
                member: member.clone(),
            },
        )?;
        let mut state = self.state.clone();
        state.nodes.insert(member.node, member);
        Ok(state)
    }
    fn transfer(&mut self, slot: u64) {
        let Some(node) = self.node.as_mut() else {
            return;
        };
        if node.raft.leader_id != self.identity.node {
            return;
        }
        let target = if slot > 0 {
            self.state.slots.get(&slot).copied()
        } else {
            node.raft
                .prs()
                .iter()
                .find(|(id, progress)| {
                    **id != self.identity.node
                        && voter(&self.conf, **id)
                        && progress.recent_active
                        && progress.matched >= self.applied
                })
                .map(|(id, _)| *id)
        };
        if let Some(target) = target.filter(|n| *n != self.identity.node) {
            node.transfer_leader(target);
        }
    }
    fn promote(&mut self) -> anyhow::Result<()> {
        if self.retired || self.conf_pending || !self.conf.voters_outgoing.is_empty() {
            return Ok(());
        }
        let Some(node) = &self.node else {
            return Ok(());
        };
        if node.raft.leader_id != self.identity.node {
            return Ok(());
        }
        self.learner_seen
            .retain(|n, _| self.conf.learners.contains(n));
        for learner in self.conf.learners.clone() {
            let Some(member) = self.state.nodes.get(&learner).cloned() else {
                continue;
            };
            let previous = self.state.slots.get(&member.id).copied().unwrap_or(0);
            let progress = node.raft.prs().get(learner);
            let active = progress.is_some_and(|p| p.recent_active);
            let seen = self
                .learner_seen
                .entry(learner)
                .or_insert_with(Instant::now);
            if active {
                *seen = Instant::now();
            }
            if seen.elapsed() > Duration::from_secs(90) {
                self.propose(
                    changes(&[(ConfChangeType::RemoveNode, learner)], false),
                    Change {
                        kind: "discard".into(),
                        member,
                        previous: 0,
                    },
                )?;
                return Ok(());
            }
            if !active
                || progress.is_none_or(|p| p.matched < self.applied)
                || previous == 0
                || previous == learner
            {
                continue;
            }
            if previous == self.identity.node {
                self.transfer(0);
                return Ok(());
            }
            self.propose(
                changes(
                    &[
                        (ConfChangeType::RemoveNode, previous),
                        (ConfChangeType::AddNode, learner),
                    ],
                    true,
                ),
                Change {
                    kind: "replace".into(),
                    member,
                    previous,
                },
            )?;
            return Ok(());
        }
        Ok(())
    }
    fn apply(&mut self, entry: Entry) -> anyhow::Result<()> {
        match entry.get_entry_type() {
            EntryType::EntryNormal => ensure!(
                entry.data.is_empty(),
                "application payloads are not allowed in leadership Raft"
            ),
            EntryType::EntryConfChangeV2 => {
                let change = ConfChangeV2::parse_from_bytes(&entry.data)?;
                if !change.context.is_empty() {
                    let context: Change = serde_json::from_slice(&change.context)?;
                    ensure!(
                        self.config.peers.contains_key(&context.member.id)
                            && context.member.node != 0,
                        "invalid committed membership"
                    );
                    match context.kind.as_str() {
                        "replace" => {
                            self.state
                                .slots
                                .insert(context.member.id, context.member.node);
                        }
                        "learner" | "discard" => (),
                        _ => bail!("unknown membership operation"),
                    }
                    self.state.nodes.insert(context.member.node, context.member);
                }
                self.conf = self.node.as_mut().unwrap().apply_conf_change(&change)?;
                self.conf_pending = false;
                if self.conf.voters_outgoing.is_empty() {
                    self.state.retired.extend(
                        self.state
                            .nodes
                            .keys()
                            .filter(|id| !known(&self.conf, **id))
                            .copied(),
                    );
                    self.state.nodes.retain(|id, _| known(&self.conf, *id));
                    self.mailbox.retain(|id, _| known(&self.conf, *id));
                    if !known(&self.conf, self.identity.node) {
                        self.retired = true;
                    }
                }
            }
            _ => bail!("unexpected Raft entry type"),
        }
        self.applied = entry.index;
        Ok(())
    }
    fn send(&mut self, message: Message) -> anyhow::Result<()> {
        let Some(record) = self.state.nodes.get(&message.to).or_else(|| {
            self.transports
                .values()
                .find(|peer| peer.node == message.to)
        }) else {
            return Ok(());
        };
        let Some(worker) = self.outgoing.get(&record.id) else {
            return Ok(());
        };
        self.sequence += 1;
        let packet = Arc::new(Packet {
            sequence: self.sequence,
            to: message.to,
            snapshot: message.get_msg_type() == MessageType::MsgSnapshot,
            generation: self.state.generation.clone().into(),
            data: message.write_to_bytes()?.into(),
        });
        let mailbox = self
            .mailbox
            .entry(message.to)
            .or_insert_with(|| ByteQueue::new(256, 512 << 10));
        mailbox.push(packet.clone(), packet.data.len());
        if worker.try_send(packet).is_err() {
            if let Some(node) = self.node.as_mut() {
                node.report_unreachable(message.to);
            }
        }
        Ok(())
    }
    fn ready(&mut self) -> anyhow::Result<()> {
        while self.node.as_ref().is_some_and(|n| n.has_ready()) {
            let mut ready = self.node.as_mut().unwrap().ready();
            if !ready.snapshot().is_empty() {
                let snapshot = ready.snapshot().clone();
                let state: Registry = serde_json::from_slice(&snapshot.data)?;
                ensure!(
                    state.generation == self.state.generation,
                    "snapshot generation mismatch"
                );
                self.storage.apply_snapshot(snapshot.clone())?;
                self.state = state;
                self.conf = snapshot.get_metadata().get_conf_state().clone();
                self.applied = snapshot.get_metadata().index;
                if !known(&self.conf, self.identity.node) {
                    self.retired = true;
                }
            }
            self.storage.append(ready.entries())?;
            if let Some(hard) = ready.hs() {
                self.storage.hard_state(hard.clone());
            }
            for message in ready
                .take_messages()
                .into_iter()
                .chain(ready.take_persisted_messages())
            {
                self.send(message)?;
            }
            let read_states = ready.take_read_states();
            for entry in ready.take_committed_entries() {
                self.apply(entry)?;
            }
            let mut light = self.node.as_mut().unwrap().advance(ready);
            if let Some(commit) = light.commit_index() {
                self.storage.commit(commit);
            }
            for message in light.take_messages() {
                self.send(message)?;
            }
            for entry in light.take_committed_entries() {
                self.apply(entry)?;
            }
            self.node.as_mut().unwrap().advance_apply_to(self.applied);
            if self.applied > 0 {
                self.storage
                    .compact(self.applied, &self.conf, serde_json::to_vec(&self.state)?)?;
            }
            let view = self.view();
            for state in read_states {
                if let Ok(id) = std::str::from_utf8(&state.request_ctx) {
                    if let Some(pending) = self.pending.remove(id) {
                        let valid = view.identity_valid
                            && view.leader_node == self.identity.node
                            && self.applied >= state.index;
                        for reply in pending.replies {
                            let result = if valid {
                                Ok(view.clone())
                            } else {
                                Err(anyhow::anyhow!("not leader"))
                            };
                            let _ = reply.send(result);
                        }
                    }
                }
            }
        }
        Ok(())
    }
    pub async fn run(
        mut self,
        mut events: mpsc::Receiver<Event>,
        cancel: CancellationToken,
    ) -> anyhow::Result<()> {
        let mut tick = tokio::time::interval(self.config.tick);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // Every waiter joins before its ReadIndex is issued. Sharing that fresh
        // round avoids a reconnect burst filling each peer's HTTP send queue.
        // Never attach a later caller to a confirmation already in flight.
        // Keep one round in flight so sustained traffic cannot build a queue
        // of obsolete confirmation heartbeats behind a slow peer link.
        let mut verify_tick = tokio::time::interval(Duration::from_millis(25));
        verify_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        self.status.send_replace(self.view());
        loop {
            tokio::select! {
                _ = cancel.cancelled() => return Ok(()),
                _ = tick.tick() => {
                    if !self.retired {
                        if let Some(node) = self.node.as_mut() { node.tick(); }
                        self.promote()?;
                    }
                    self.pending.retain(|_, p| p.replies.iter().any(|r| !r.is_closed()) && p.since.elapsed() < Duration::from_secs(4));
                },
                _ = verify_tick.tick(), if !self.verify_queue.is_empty() && self.pending.is_empty() => {
                    let replies = std::mem::take(&mut self.verify_queue).into_iter().filter(|r| !r.is_closed()).collect::<Vec<_>>();
                    let view = self.view();
                    if !view.identity_valid || view.leader_node != self.identity.node || view.role != "StateLeader" {
                        for reply in replies { let _=reply.send(Err(anyhow::anyhow!("not leader"))); }
                    } else if !replies.is_empty() {
                        let id = random_id();
                        self.node.as_mut().unwrap().read_index(id.as_bytes().to_vec());
                        self.pending.insert(id, Pending { since: Instant::now(), replies });
                    }
                },
                event = events.recv() => match event {
                    None => return Ok(()),
                    Some(Event::Bootstrap(manifest, reply)) => { let result = self.bootstrap(manifest); let _ = reply.send(result); },
                    Some(Event::Join(member, reply)) => { let result = self.join(member); let _ = reply.send(result); },
                    Some(Event::Adopt(state)) => if self.node.is_none() { self.initialize(state, None)?; },
                    Some(Event::Message(message,sender,_permit)) => if !self.retired {
                        if let Some(node) = self.node.as_mut() {
                            if let Some(sender) = sender {
                                if let Some(old) = self.transports.insert(sender.id, sender) {
                                    if !self.state.nodes.contains_key(&old.node) { self.mailbox.remove(&old.node); }
                                }
                            }
                            match node.step(message) { Ok(()) | Err(raft::Error::StepPeerNotFound) => (), Err(e) => return Err(e.into()) }
                        }
                    },
                    Some(Event::Verify(reply)) => {
                        let view = self.view();
                        if !view.identity_valid || view.leader_node != self.identity.node || view.role != "StateLeader" {
                            let _ = reply.send(Err(anyhow::anyhow!("not leader")));
                        } else if self.verify_queue.len() + self.pending.values().map(|p| p.replies.len()).sum::<usize>() >= 128 {
                            let _ = reply.send(Err(anyhow::anyhow!("busy")));
                        } else {
                            self.verify_queue.push(reply);
                        }
                    },
                    Some(Event::Transfer(slot)) => self.transfer(slot),
                    Some(Event::Fence(term, reply)) => {
                        let result = if term > 0 && term < 1 << 52 {
                            self.minimum_term = self.minimum_term.max(term);
                            if let Some(node) = self.node.as_mut() {
                                if node.raft.term < self.minimum_term { node.raft.become_follower(self.minimum_term, 0); }
                            }
                            Ok(())
                        } else { Err(anyhow::anyhow!("invalid fencing term")) };
                        let _ = reply.send(result);
                    },
                    Some(Event::Pull { node, boot, generation, reply }) => {
                        let result = if self.state.generation != generation || !self.state.nodes.get(&node).is_some_and(|n| n.boot == boot) {
                            Err(anyhow::anyhow!("unknown incarnation"))
                        } else {
                            let mut out = Vec::new();
                            if let Some(queue) = self.mailbox.get_mut(&node) {
                                for _ in 0..64 { let Some(packet) = queue.pop() else { break; };
                                    out.push(Envelope { sequence: packet.sequence, data: STANDARD.encode(&packet.data) }); }
                            }
                            Ok(out)
                        };
                        let _ = reply.send(result);
                    },
                    Some(Event::Delivered { to, sequence, snapshot, ok }) => {
                        if ok { if let Some(queue) = self.mailbox.get_mut(&to) { queue.retain(|p| p.sequence != sequence); } }
                        if let Some(node) = self.node.as_mut() {
                            if !ok { node.report_unreachable(to); }
                            if snapshot { node.report_snapshot(to, if ok { SnapshotStatus::Finish } else { SnapshotStatus::Failure }); }
                        }
                    },
                }
            }
            self.ready()?;
            if self
                .node
                .as_ref()
                .is_some_and(|n| n.raft.state != StateRole::Leader)
            {
                self.conf_pending = false;
            }
            self.status.send_replace(self.view());
        }
    }
}
fn voter(conf: &ConfState, id: u64) -> bool {
    conf.voters.contains(&id) || conf.voters_outgoing.contains(&id)
}
fn known(conf: &ConfState, id: u64) -> bool {
    voter(conf, id) || conf.learners.contains(&id) || conf.learners_next.contains(&id)
}
fn changes(items: &[(ConfChangeType, u64)], joint: bool) -> ConfChangeV2 {
    let mut change = ConfChangeV2::default();
    if joint {
        change.set_transition(ConfChangeTransition::Implicit);
    }
    for (kind, id) in items {
        let mut item = ConfChangeSingle::default();
        item.set_change_type(*kind);
        item.node_id = *id;
        change.mut_changes().push(item);
    }
    change
}
