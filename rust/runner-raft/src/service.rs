use crate::{
    config::Config,
    engine::{Core, Event},
    model::*,
    transport,
};
use axum::{
    Json, Router,
    body::to_bytes,
    extract::{Request, State},
    http::{Method, StatusCode},
    response::{IntoResponse, Response},
};
use protobuf::Message as _;
use raft::prelude::{Message, MessageType};
use runner_core::{auth::equal_secret, http_client, random_id};
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use tokio::sync::{Semaphore, mpsc, oneshot, watch};
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
pub struct RaftService {
    config: Arc<Config>,
    events: mpsc::Sender<Event>,
    status: watch::Receiver<Arc<View>>,
    bodies: Arc<Semaphore>,
    cancel: CancellationToken,
    client: reqwest::Client,
}
impl RaftService {
    pub fn start(config: Config, cancel: CancellationToken) -> anyhow::Result<Self> {
        let config = Arc::new(config);
        let bytes = uuid::Uuid::new_v4().into_bytes();
        let node = u64::from_be_bytes([
            0, 0, bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5],
        ])
        .max(1);
        let identity = Identity {
            node,
            id: config.id,
            boot: random_id(),
        };
        let (events, receiver) = mpsc::channel(64);
        let (sender, status) = watch::channel(Arc::new(View::default()));
        let client = http_client(Duration::from_secs(4))?;
        let mut outgoing = BTreeMap::new();
        for member in &config.members {
            let (tx, rx) = mpsc::channel(16);
            outgoing.insert(member.id, tx);
            tokio::spawn(transport::worker(
                config.clone(),
                identity.clone(),
                member.id,
                client.clone(),
                rx,
                events.clone(),
                cancel.clone(),
            ));
        }
        let core = Core::new(config.clone(), identity.clone(), sender, outgoing);
        let stop = cancel.clone();
        tokio::spawn(async move {
            if let Err(error) = core.run(receiver, stop.clone()).await {
                eprintln!("Raft stopped: {error}");
                stop.cancel();
            }
        });
        tokio::spawn(transport::network(
            config.clone(),
            identity,
            client.clone(),
            status.clone(),
            events.clone(),
            cancel.clone(),
        ));
        Ok(Self {
            config,
            events,
            status,
            bodies: Arc::new(Semaphore::new(4)),
            cancel,
            client,
        })
    }
    pub fn view(&self) -> Arc<View> {
        self.status.borrow().clone()
    }
    pub fn router(&self) -> Router {
        Router::new().fallback(endpoint).with_state(self.clone())
    }
    pub async fn verify(&self) -> anyhow::Result<Arc<View>> {
        let (reply, receive) = oneshot::channel();
        tokio::time::timeout(Duration::from_secs(4), async {
            self.events
                .send(Event::Verify(reply))
                .await
                .map_err(|_| anyhow::anyhow!("voter stopped"))?;
            receive
                .await
                .map_err(|_| anyhow::anyhow!("quorum unavailable"))?
        })
        .await
        .map_err(|_| anyhow::anyhow!("quorum unavailable"))?
    }
    pub async fn transfer(&self, slot: u64) -> anyhow::Result<()> {
        self.events
            .send(Event::Transfer(slot))
            .await
            .map_err(|_| anyhow::anyhow!("voter stopped"))
    }
    pub async fn fence(&self, term: u64) -> anyhow::Result<()> {
        let (reply, receive) = oneshot::channel();
        self.events
            .send(Event::Fence(term, reply))
            .await
            .map_err(|_| anyhow::anyhow!("voter stopped"))?;
        tokio::time::timeout(Duration::from_secs(4), receive).await??
    }
    pub async fn handle(&self, request: Request) -> Response {
        match tokio::time::timeout(Duration::from_secs(5), self.handle_inner(request)).await {
            Ok(response) => response,
            Err(_) => error(
                StatusCode::SERVICE_UNAVAILABLE,
                "voter busy or quorum unavailable",
            ),
        }
    }
    async fn handle_inner(&self, request: Request) -> Response {
        if self.cancel.is_cancelled() {
            return error(StatusCode::SERVICE_UNAVAILABLE, "voter stopped");
        }
        let (parts, body) = request.into_parts();
        let header = |key: &str| {
            parts
                .headers
                .get(key)
                .and_then(|v| v.to_str().ok())
                .unwrap_or("")
        };
        if !equal_secret(header("x-cp-token"), &self.config.secret) {
            return error(StatusCode::UNAUTHORIZED, "unauthorized");
        }
        if !header("x-cp-cluster").is_empty() && header("x-cp-cluster") != self.config.cluster {
            return error(StatusCode::CONFLICT, "wrong cluster");
        }
        let path = parts.uri.path();
        let view = self.view();
        let query: BTreeMap<_, _> = parts.uri.query().map(|q| url_query(q)).unwrap_or_default();
        match path {
            "/status" | "/_cp/raft-status" => Json(view).into_response(),
            "/verify" => match self.verify().await {
                Ok(v) => Json(v).into_response(),
                Err(e) => error(
                    if e.to_string() == "not leader" {
                        StatusCode::CONFLICT
                    } else {
                        StatusCode::SERVICE_UNAVAILABLE
                    },
                    &e.to_string(),
                ),
            },
            "/transfer" | "/_cp/transfer" => {
                if parts.method != Method::POST {
                    return error(StatusCode::METHOD_NOT_ALLOWED, "POST required");
                }
                let slot = query.get("to").and_then(|s| s.parse().ok()).unwrap_or(0);
                match self.transfer(slot).await {
                    Ok(()) => Json(serde_json::json!({"ok": true})).into_response(),
                    Err(e) => error(StatusCode::SERVICE_UNAVAILABLE, &e.to_string()),
                }
            }
            "/pull" | "/_cp/pull" => {
                let (reply, receive) = oneshot::channel();
                let event = Event::Pull {
                    node: query.get("node").and_then(|s| s.parse().ok()).unwrap_or(0),
                    boot: query.get("boot").cloned().unwrap_or_default(),
                    generation: header("x-cp-generation").into(),
                    reply,
                };
                if self.events.send(event).await.is_err() {
                    return error(StatusCode::SERVICE_UNAVAILABLE, "voter stopped");
                }
                match receive.await {
                    Ok(Ok(messages)) => Json(messages).into_response(),
                    _ => error(StatusCode::CONFLICT, "unknown incarnation"),
                }
            }
            "/bootstrap" | "/_cp/bootstrap" | "/join" | "/_cp/join" => {
                if parts.method != Method::POST {
                    return error(StatusCode::METHOD_NOT_ALLOWED, "POST required");
                }
                let Ok(_permit) = self.bodies.clone().try_acquire_owned() else {
                    return error(StatusCode::SERVICE_UNAVAILABLE, "busy");
                };
                let Ok(raw) = to_bytes(body, 1 << 20).await else {
                    return error(
                        StatusCode::PAYLOAD_TOO_LARGE,
                        "invalid or oversized request",
                    );
                };
                if path.ends_with("bootstrap") {
                    let Ok(manifest) = serde_json::from_slice(&raw) else {
                        return error(StatusCode::BAD_REQUEST, "invalid bootstrap manifest");
                    };
                    let (reply, receive) = oneshot::channel();
                    if self
                        .events
                        .send(Event::Bootstrap(manifest, reply))
                        .await
                        .is_err()
                    {
                        return error(StatusCode::SERVICE_UNAVAILABLE, "voter stopped");
                    }
                    match receive.await {
                        Ok(Ok(())) => Json(serde_json::json!({"ok": true})).into_response(),
                        Ok(Err(e)) => error(StatusCode::CONFLICT, &e.to_string()),
                        _ => error(StatusCode::SERVICE_UNAVAILABLE, "voter stopped"),
                    }
                } else {
                    // Admission needs a fresh quorum, even if the last status
                    // still names this process as leader after a partition.
                    if self.verify().await.is_err() {
                        return error(StatusCode::SERVICE_UNAVAILABLE, "quorum unavailable");
                    }
                    let Ok(identity) = serde_json::from_slice(&raw) else {
                        return error(StatusCode::BAD_REQUEST, "invalid joining incarnation");
                    };
                    let (reply, receive) = oneshot::channel();
                    if self
                        .events
                        .send(Event::Join(identity, reply))
                        .await
                        .is_err()
                    {
                        return error(StatusCode::SERVICE_UNAVAILABLE, "voter stopped");
                    }
                    match receive.await {
                        Ok(Ok(state)) => Json(state).into_response(),
                        Ok(Err(e)) => error(StatusCode::CONFLICT, &e.to_string()),
                        _ => error(StatusCode::SERVICE_UNAVAILABLE, "voter stopped"),
                    }
                }
            }
            "/message" | "/_cp/raft" => {
                let Ok(_permit) = self.bodies.clone().try_acquire_owned() else {
                    return error(StatusCode::SERVICE_UNAVAILABLE, "busy");
                };
                let Ok(raw) = to_bytes(body, 1 << 20).await else {
                    return error(
                        StatusCode::PAYLOAD_TOO_LARGE,
                        "invalid or oversized message",
                    );
                };
                let Ok(message) = Message::parse_from_bytes(&raw) else {
                    return error(StatusCode::BAD_REQUEST, "invalid message");
                };
                if message.to != view.node
                    || view.generation.is_empty()
                    || header("x-cp-generation") != view.generation
                {
                    return error(StatusCode::CONFLICT, "wrong incarnation or generation");
                }
                let registered = view
                    .registry
                    .nodes
                    .get(&message.from)
                    .is_some_and(|n| n.boot == header("x-cp-boot"));
                let sender = if registered {
                    None
                } else {
                    let Some(sender) = self
                        .current_leader_sender(
                            &message,
                            &view,
                            header("x-cp-from"),
                            header("x-cp-boot"),
                        )
                        .await
                    else {
                        return error(StatusCode::CONFLICT, "unknown sender incarnation");
                    };
                    Some(sender)
                };
                // Local-only message types must not be injected over HTTP.
                if matches!(
                    message.get_msg_type(),
                    MessageType::MsgHup
                        | MessageType::MsgBeat
                        | MessageType::MsgUnreachable
                        | MessageType::MsgSnapStatus
                        | MessageType::MsgCheckQuorum
                        | MessageType::MsgPropose
                ) {
                    return error(StatusCode::BAD_REQUEST, "local message refused");
                }
                match self
                    .events
                    .send(Event::Message(message, sender, Some(_permit)))
                    .await
                {
                    Ok(()) => StatusCode::NO_CONTENT.into_response(),
                    Err(_) => error(StatusCode::SERVICE_UNAVAILABLE, "voter stopped"),
                }
            }
            _ => error(StatusCode::NOT_FOUND, "not found"),
        }
    }
    async fn current_leader_sender(
        &self,
        message: &Message,
        own: &View,
        slot: &str,
        boot: &str,
    ) -> Option<Identity> {
        if !matches!(
            message.get_msg_type(),
            MessageType::MsgSnapshot | MessageType::MsgAppend | MessageType::MsgHeartbeat
        ) || message.term < own.term
        {
            return None;
        }
        let Some(origin) = slot
            .parse::<u64>()
            .ok()
            .and_then(|id| self.config.peers.get(&id))
        else {
            return None;
        };
        // A lagging voter may not yet know the physical identity of a newly
        // elected replacement. Authenticate it through the configured logical
        // peer, rather than requiring the membership snapshot it is trying to send.
        // This does not grant scheduler authority or bypass Raft snapshot validation.
        let response = self
            .client
            .get(format!("{origin}/_cp/raft-status"))
            .header("x-cp-token", &self.config.secret)
            .header("x-cp-from", self.config.id)
            .header("x-cp-cluster", &self.config.cluster)
            .timeout(Duration::from_millis(1200))
            .send()
            .await;
        let Ok(response) = response else {
            return None;
        };
        let Ok(remote) = runner_core::response_json::<View>(response, 128 << 10).await else {
            return None;
        };
        (remote.id.to_string() == slot
            && remote.node == message.from
            && remote.boot == boot
            && remote.generation == own.generation
            && remote.identity_valid
            && remote.leader_node == remote.node
            && remote.role == "StateLeader"
            && remote.term >= message.term)
            .then_some(Identity {
                id: remote.id,
                node: remote.node,
                boot: remote.boot,
            })
    }
}
async fn endpoint(State(service): State<RaftService>, request: Request) -> Response {
    service.handle(request).await
}
fn error(status: StatusCode, message: &str) -> Response {
    (status, Json(serde_json::json!({"error": message}))).into_response()
}
fn url_query(query: &str) -> BTreeMap<String, String> {
    reqwest::Url::parse(&format!("http://local/?{query}"))
        .map(|url| url.query_pairs().into_owned().collect())
        .unwrap_or_default()
}
