use crate::{
    api::{Output, Request},
    authority::{Authority, Proof},
    control::{Control, s},
    error::{HttpError, http},
    hub::Hub,
    storage::Store,
    tail::Tail,
};
use serde_json::{Value, json};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::{mpsc, oneshot, watch};
use tokio_util::sync::CancellationToken;
#[cfg(test)]
mod tests;
type Reply<T> = oneshot::Sender<anyhow::Result<T>>;
const MAX_BATCH: usize = 16;
enum Command {
    Api(Request, Reply<Output>),
    AttachLog {
        session: String,
        run: Option<String>,
        reply: Reply<(Value, Vec<String>)>,
    },
    EndLog(String),
    Location(String, Option<String>),
}
impl Command {
    fn batchable(&self) -> bool {
        let Self::Api(request, _) = self else {
            return false;
        };
        let path = request.path.trim_end_matches('/');
        let path = path.strip_prefix("/admin").unwrap_or(path);
        match request.method {
            axum::http::Method::GET => {
                matches!(
                    path,
                    "/api/status" | "/api/events" | "/api/metrics" | "/internal/dns"
                )
            }
            axum::http::Method::POST => path == "/api/sync",
            _ => false,
        }
    }
    fn metrics(&self) -> bool {
        matches!(self, Self::Api(request, _) if request.method == axum::http::Method::GET && matches!(request.path.trim_end_matches('/'), "/api/metrics" | "/admin/api/metrics"))
    }
}
struct PendingApi {
    request: Request,
    reply: Reply<Output>,
    result: anyhow::Result<Output>,
    is_sync: bool,
}
#[derive(Clone)]
pub struct Service {
    commands: mpsc::Sender<Command>,
    pub hub: Hub,
    pub cancel: CancellationToken,
    pub ready: watch::Receiver<bool>,
    pub authority: Option<(Authority, Proof)>,
}
impl Service {
    pub async fn open(
        env: std::collections::BTreeMap<String, String>,
        authority: Option<(Authority, Proof)>,
        tail: Arc<Mutex<Tail>>,
        cancel: CancellationToken,
    ) -> anyhow::Result<Self> {
        let mut store = Store::open(&env).await?;
        store.authority = authority.clone();
        let c = Control::new(store, env, runner_core::now_ms())?;
        let (commands, rx) = mpsc::channel(32);
        let (ready_tx, ready) = watch::channel(false);
        let hub = Hub::new(cancel.clone());
        let service = Self {
            commands,
            hub,
            cancel,
            ready,
            authority,
        };
        let actor = service.clone();
        tokio::spawn(async move {
            if let Err(_error) = actor.run(c, tail, rx, ready_tx).await {
                eprintln!("scheduler stopped: configuration or authority unavailable");
            }
            actor.cancel.cancel();
        });
        Ok(service)
    }
    pub fn from_control(c: Control, cancel: CancellationToken) -> Self {
        let (commands, rx) = mpsc::channel(32);
        let (ready_tx, ready) = watch::channel(true);
        let hub = Hub::new(cancel.clone());
        let service = Self {
            commands,
            hub,
            cancel,
            ready,
            authority: None,
        };
        let actor = service.clone();
        tokio::spawn(async move {
            let _ = actor
                .run(c, Arc::new(Mutex::new(Tail::default())), rx, ready_tx)
                .await;
            actor.cancel.cancel();
        });
        service
    }
    pub async fn check(&self) -> anyhow::Result<Option<Proof>> {
        if self.cancel.is_cancelled() {
            return Err(http(503, "primary unavailable; retry shortly"));
        }
        if let Some((authority, proof)) = &self.authority {
            Ok(Some(authority.check(Some(proof)).await.map_err(|_| {
                http(503, "control-plane leadership is no longer valid")
            })?))
        } else {
            Ok(None)
        }
    }
    async fn send(&self, command: Command) -> anyhow::Result<()> {
        self.commands
            .try_send(command)
            .map_err(|_| http(503, "scheduler busy; retry shortly"))
    }
    pub async fn request(&self, request: Request) -> anyhow::Result<Output> {
        let (reply, rx) = oneshot::channel();
        self.send(Command::Api(request, reply)).await?;
        tokio::select! {_ = self.cancel.cancelled()=>Err(http(503,"primary unavailable")),result=tokio::time::timeout(Duration::from_secs(60),rx)=>result.map_err(|_|http(503,"scheduler timeout"))?.map_err(|_|http(503,"scheduler unavailable"))?}
    }
    pub async fn attach_log(
        &self,
        session: String,
        run: Option<String>,
    ) -> anyhow::Result<(Value, Vec<String>)> {
        let (reply, rx) = oneshot::channel();
        self.send(Command::AttachLog {
            session,
            run,
            reply,
        })
        .await?;
        tokio::time::timeout(Duration::from_secs(10), rx)
            .await
            .map_err(|_| http(503, "scheduler timeout"))?
            .map_err(|_| http(503, "scheduler unavailable"))?
    }
    pub fn end_log(&self, session: &str) {
        self.hub.end_log(session);
        let _ = self.commands.try_send(Command::EndLog(session.into()));
    }
    fn notify(&self, c: &mut Control) {
        for run in std::mem::take(&mut c.wakes) {
            self.hub.wake(&run);
        }
    }
    async fn run(
        &self,
        mut c: Control,
        _tail: Arc<Mutex<Tail>>,
        mut rx: mpsc::Receiver<Command>,
        ready: watch::Sender<bool>,
    ) -> anyhow::Result<()> {
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let jobs = Arc::new(tokio::sync::Semaphore::new(4));
        let mut tasks = tokio::task::JoinSet::new();
        let mut deferred = None;
        loop {
            if self.cancel.is_cancelled() {
                break;
            }
            // Give a due alarm its turn even when reports keep the queue full.
            let next = tokio::select! {biased;_=self.cancel.cancelled()=>break,done=tasks.join_next(),if !tasks.is_empty()=>{let _=done;continue;},_=interval.tick()=>None,cmd=async{if deferred.is_some(){deferred.take()}else{rx.recv().await}}=>cmd};
            let mut batch = Vec::with_capacity(MAX_BATCH);
            let batchable = next.as_ref().is_some_and(Command::batchable);
            let mut metrics = next.as_ref().is_some_and(Command::metrics);
            batch.push(next);
            if batchable {
                while batch.len() < MAX_BATCH {
                    let Ok(command) = rx.try_recv() else { break };
                    if !command.batchable() || (metrics && command.metrics()) {
                        deferred = Some(command);
                        break;
                    }
                    metrics |= command.metrics();
                    batch.push(Some(command));
                }
            }
            // Close the batch before issuing its fresh quorum confirmation.
            // New arrivals wait for a later batch; no caller borrows an older
            // confirmation. Mutations/history reads run separately, and at
            // most one metrics response is retained in each batch.
            self.check().await?;
            let mut replies = Vec::new();
            for next in batch {
                match next {
                    Some(Command::Api(request, reply)) => {
                        if reply.is_closed() {
                            continue;
                        }
                        let now = runner_core::now_ms();
                        c.store.begin()?;
                        let changes = c.store.changes();
                        let is_sync = request.method == axum::http::Method::POST
                            && request.path.trim_end_matches('/') == "/api/sync";
                        let result = c.api(&request, now).await;
                        match &result {
                            Ok(_) => c.store.commit().await?,
                            Err(error) => {
                                let status = error
                                    .downcast_ref::<HttpError>()
                                    .map(|e| e.status)
                                    .unwrap_or(500);
                                if is_sync && status == 503 {
                                    c.store.commit().await?;
                                } else {
                                    let touched = c.store.changes() != changes;
                                    c.store.rollback()?;
                                    // A retryable API response (recovery, capacity,
                                    // tunnel discovery) must not restart recovery.
                                    // Authority and storage failures still stop the
                                    // actor through the checks outside this branch.
                                    if status >= 500 && status != 503 {
                                        return Err(anyhow::anyhow!("scheduler operation failed"));
                                    }
                                    if touched {
                                        c.reload_rows()?;
                                    }
                                }
                            }
                        }
                        replies.push(PendingApi {
                            request,
                            reply,
                            result,
                            is_sync,
                        });
                    }
                    Some(Command::AttachLog {
                        session,
                        run,
                        reply,
                    }) => {
                        let result = c.attach_log(&session, run.as_deref(), runner_core::now_ms());
                        let _ = reply.send(result);
                    }
                    Some(Command::EndLog(id)) => {
                        c.log_sessions.remove(&id);
                    }
                    Some(Command::Location(ip, location)) => {
                        c.record_location(&ip, location, runner_core::now_ms())?;
                    }
                    None => {
                        let now = runner_core::now_ms();
                        c.store.begin()?;
                        c.alarm(now).await?;
                        c.cleanup(now)?;
                        c.store.commit().await?;
                        let _ = ready.send(c.tunnels_ready);
                    }
                }
            }
            if !replies.is_empty() {
                // Plans and read responses remain private until a second fresh
                // confirmation covers the completed batch. D1 commits retain
                // their own immediate authority guard inside Store::commit.
                let proof = self.check().await?;
                let mut restart = false;
                for mut pending in replies {
                    if let Ok(Output::Json(value)) = &mut pending.result {
                        if pending.is_sync {
                            if let Some(proof) = &proof {
                                value["leadership"] = json!(proof);
                            }
                        }
                        if pending.request.path.ends_with("/status") {
                            value["controlPlane"] = json!({"mode":if self.authority.is_some(){"etcd-raft"}else{"native-v1"},"runtime":"rust","owner":c.e("CONTROLLER_KIND"),"name":if c.e("CONTROLLER_NAME").is_empty(){c.e("CONTROL_HOST")}else{c.e("CONTROLLER_NAME")},"replica":c.en("CP_NODE_ID",1),"replicas":c.en("CP_REPLICAS",1),"clusterView":"/#/fleet","configStorage":if c.e("CONFIG_STORAGE")=="d1"{"cloudflare-d1"}else{"local-sqlite"},"enabled":true,"agentsConnected":self.hub.agents(),"runtimeStorage":"memory","metricsHistoryMinutes":c.en("MEMORY_HISTORY_MIN",360),"tunnelStorage":"memory","tunnelsReady":c.tunnels_ready,"tunnelsDiscovered":c.slots.len(),"tunnelRecoveryError":c.tunnel_recovery_error,"recoveryUntil":c.recovering_until,"recoveryMissing":c.recovery_missing});
                        }
                    }
                    restart |= matches!(&pending.result, Ok(Output::Restart));
                    let _ = pending.reply.send(pending.result);
                }
                if restart {
                    tokio::time::sleep(Duration::from_millis(300)).await;
                    break;
                }
            }
            self.notify(&mut c);
            // Small best-effort background jobs never hold the scheduler while
            // waiting on remote geolocation or an operator-configured webhook.
            for ip in std::mem::take(&mut c.geo_lookups) {
                let Ok(permit) = jobs.clone().try_acquire_owned() else {
                    c.geo_pending.remove(&ip);
                    continue;
                };
                let client = c.http.clone();
                let commands = self.commands.clone();
                let cancel = self.cancel.clone();
                tasks.spawn(async move{let _permit=permit;let response=tokio::select!{_=cancel.cancelled()=>return,r=client.get(format!("http://ip-api.com/json/{ip}?fields=status,city,regionName,countryCode")).timeout(Duration::from_secs(6)).send()=>r};let location=match response{Ok(r)=>runner_core::response_json::<Value>(r,16<<10).await.ok().filter(|v|v["status"]=="success").map(|v|[s(&v,"city"),s(&v,"regionName"),s(&v,"countryCode")].into_iter().filter(|s|!s.is_empty()).collect::<Vec<_>>().join(", ")),Err(_)=>None};let _=commands.try_send(Command::Location(ip,location));});
            }
            for body in std::mem::take(&mut c.alerts) {
                if c.e("ALERT_WEBHOOK").is_empty() {
                    continue;
                }
                let Ok(permit) = jobs.clone().try_acquire_owned() else {
                    continue;
                };
                let client = c.http.clone();
                let url = c.e("ALERT_WEBHOOK").to_owned();
                let cancel = self.cancel.clone();
                tasks.spawn(async move{let _permit=permit;tokio::select!{_=cancel.cancelled()=>{},_=client.post(url).json(&body).timeout(Duration::from_secs(8)).send()=>{}}});
            }
        }
        Ok(())
    }
}
