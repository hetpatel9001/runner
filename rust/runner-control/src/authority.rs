use crate::etcd::RaftService;
use serde::{Deserialize, Serialize};
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Proof {
    pub cluster: String,
    pub node: u64,
    pub boot: String,
    pub term: u64,
    pub epoch: u64,
}
struct Lease {
    proof: Proof,
    deadline: Instant,
    wall_deadline: u64,
}
#[derive(Clone)]
pub struct Authority {
    raft: RaftService,
    cluster: String,
    slot: u64,
    current: Arc<Mutex<Option<Lease>>>,
    forced: bool,
    force_proof: Arc<Mutex<Option<Proof>>>,
    suppressed: Arc<AtomicBool>,
}
impl Authority {
    pub fn new(raft: RaftService, cluster: String, slot: u64, forced: bool) -> Self {
        Self {
            raft,
            cluster,
            slot,
            current: Arc::new(Mutex::new(None)),
            forced,
            force_proof: Default::default(),
            suppressed: Default::default(),
        }
    }
    pub fn forced(&self) -> bool {
        self.forced
    }
    pub fn forced_proof(&self) -> Option<Proof> {
        self.force_proof.lock().unwrap().clone()
    }
    pub fn suppress(&self, value: bool) {
        self.suppressed.store(value, Ordering::Relaxed);
    }
    pub fn suppressed(&self) -> bool {
        self.suppressed.load(Ordering::Relaxed)
    }
    pub fn clear(&self) {
        *self.current.lock().unwrap() = None;
    }
    pub fn live(&self) -> Option<Proof> {
        if self.suppressed() {
            return None;
        }
        self.current
            .lock()
            .unwrap()
            .as_ref()
            .filter(|l| Instant::now() < l.deadline && runner_core::now_ms() < l.wall_deadline)
            .map(|l| l.proof.clone())
    }
    pub async fn renew(&self) -> anyhow::Result<Proof> {
        anyhow::ensure!(!self.suppressed(), "another forced controller is active");
        let start = Instant::now();
        let wall = runner_core::now_ms();
        let proof = if self.forced {
            // A new operator incarnation fences agents' previously accepted plans.
            // Keep this proof stable until this process is restarted without the flag.
            self.force_proof
                .lock()
                .unwrap()
                .get_or_insert_with(|| {
                    let epoch = wall.max(self.raft.view().term.saturating_add(1));
                    Proof {
                        cluster: self.cluster.clone(),
                        node: self.slot,
                        boot: format!("forced-{}", runner_core::random_id()),
                        term: epoch,
                        epoch,
                    }
                })
                .clone()
        } else {
            let raft = self.raft.verify().await?;
            anyhow::ensure!(
                raft.id == self.slot && raft.leader == raft.id && raft.identity_valid,
                "Raft leadership unavailable"
            );
            Proof {
                cluster: self.cluster.clone(),
                node: raft.id,
                boot: raft.boot.clone(),
                term: raft.term,
                epoch: raft.term,
            }
        };
        *self.current.lock().unwrap() = Some(Lease {
            proof: proof.clone(),
            deadline: start + Duration::from_secs(14),
            wall_deadline: wall + 14_000,
        });
        if self.live().is_none() {
            self.clear();
            anyhow::bail!("leadership confirmation expired in transit");
        }
        Ok(proof)
    }
    pub async fn check(&self, expected: Option<&Proof>) -> anyhow::Result<Proof> {
        let before = self
            .live()
            .ok_or_else(|| anyhow::anyhow!("leadership lease unavailable"))?;
        if self.forced {
            anyhow::ensure!(
                self.forced_proof().as_ref() == Some(&before),
                "forced authority changed"
            );
        } else {
            let raft = self.raft.verify().await?;
            anyhow::ensure!(
                self.live().as_ref() == Some(&before)
                    && raft.leader == before.node
                    && raft.id == before.node
                    && raft.boot == before.boot
                    && raft.term == before.term
                    && raft.identity_valid,
                "leadership changed"
            );
        }
        anyhow::ensure!(
            self.live().as_ref() == Some(&before),
            "authority expired or suppressed"
        );
        anyhow::ensure!(
            expected.is_none_or(|p| p == &before),
            "obsolete control-plane authority"
        );
        Ok(before)
    }
}
