//! Process/HTTP adapter for the official go.etcd.io/raft/v3 implementation.
//! No election, lease, vote counting or replication algorithm lives here.
mod config;
pub mod model;
use axum::{
    Json,
    body::{Body, to_bytes},
    http::{Request, StatusCode},
    response::{IntoResponse, Response},
};
pub use config::Config;
use model::View;
use serde::de::DeserializeOwned;
use serde_json::json;
use std::{process::Stdio, sync::Arc, time::Duration};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
pub struct RaftService {
    config: Arc<Config>,
    origin: String,
    client: reqwest::Client,
    status: watch::Receiver<Arc<View>>,
    cancel: CancellationToken,
}
impl RaftService {
    pub fn start(config: Config, cancel: CancellationToken) -> anyhow::Result<Self> {
        // The private endpoint has no externally visible port. Tests and local
        // multi-replica deployments may select an explicit loopback port.
        let port = match config.env.get("CP_RAFT_PORT") {
            Some(value) => value.parse::<u16>()?,
            None => std::net::TcpListener::bind("127.0.0.1:0")?
                .local_addr()?
                .port(),
        };
        anyhow::ensure!(port > 0, "invalid CP_RAFT_PORT");
        let binary = config
            .env
            .get("CP_ETCD_RAFT_BINARY")
            .cloned()
            .or_else(|| std::env::var("CP_ETCD_RAFT_BINARY").ok())
            .unwrap_or_else(|| "/usr/local/bin/runner-etcd-raft".into());
        let mut command = tokio::process::Command::new(binary);
        command
            .env_clear()
            .envs(
                config
                    .env
                    .iter()
                    .filter(|(key, _)| key.starts_with("CP_") || key.as_str() == "GOMAXPROCS"),
            )
            .env("CP_RAFT_PORT", port.to_string())
            .env("CP_RAFT_ALLOW_CLAIMS", "0")
            .env("CP_PARENT_PID", std::process::id().to_string())
            .stdin(Stdio::null())
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .kill_on_drop(true);
        let mut child = command.spawn().map_err(|_| {
            anyhow::anyhow!("could not start official etcd Raft helper; check CP_ETCD_RAFT_BINARY")
        })?;
        let (sender, status) = watch::channel(Arc::new(View {
            phase: "starting".into(),
            role: "Starting".into(),
            ..Default::default()
        }));
        let service = Self {
            config: Arc::new(config),
            origin: format!("http://127.0.0.1:{port}"),
            client: runner_core::http_client(Duration::from_secs(5))?,
            status,
            cancel: cancel.clone(),
        };
        let running = service.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(100));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    _=cancel.cancelled()=>{let _=child.kill().await;break;},
                    _=child.wait()=>{cancel.cancel();break;},
                    _=interval.tick()=>{
                        let view=tokio::time::timeout(Duration::from_millis(800),running.json::<View>("/status",None)).await.ok().and_then(Result::ok);
                        match view {
                            Some(view)=>{sender.send_replace(Arc::new(view));},
                            None=>{
                                let mut view=(*sender.borrow()).as_ref().clone();
                                view.role="Unavailable".into();view.leader=0;view.identity_valid=false;
                                sender.send_replace(Arc::new(view));
                            }
                        }
                    }
                }
            }
            let mut view = (*sender.borrow()).as_ref().clone();
            view.role = "Stopped".into();
            view.leader = 0;
            view.identity_valid = false;
            sender.send_replace(Arc::new(view));
        });
        Ok(service)
    }
    pub fn view(&self) -> Arc<View> {
        self.status.borrow().clone()
    }
    pub async fn changed(&self) {
        let mut status = self.status.clone();
        let _ = status.changed().await;
    }
    pub fn router(&self) -> axum::Router {
        let service = self.clone();
        axum::Router::new().fallback(move |request: Request<Body>| {
            let service = service.clone();
            async move { service.handle(request).await }
        })
    }
    async fn json<T: DeserializeOwned>(
        &self,
        path: &str,
        body: Option<serde_json::Value>,
    ) -> anyhow::Result<T> {
        anyhow::ensure!(!self.cancel.is_cancelled(), "Raft helper stopped");
        let mut request = if body.is_some() {
            self.client.post(format!("{}{path}", self.origin))
        } else {
            self.client.get(format!("{}{path}", self.origin))
        };
        request = request
            .header("x-cp-token", &self.config.secret)
            .header("x-cp-cluster", &self.config.cluster);
        if let Some(body) = body {
            request = request.json(&body);
        }
        runner_core::response_json(request.send().await?, 4 << 20).await
    }
    pub async fn verify(&self) -> anyhow::Result<Arc<View>> {
        let view: View = self.json("/verify", None).await?;
        anyhow::ensure!(
            view.identity_valid && view.leader == view.id && view.role == "StateLeader",
            "not Raft primary"
        );
        Ok(Arc::new(view))
    }
    pub async fn transfer(&self, slot: u64) -> anyhow::Result<()> {
        let _: serde_json::Value = self
            .json(&format!("/transfer?to={slot}"), Some(json!({})))
            .await?;
        Ok(())
    }
    pub async fn fence(&self, term: u64) -> anyhow::Result<()> {
        let _: serde_json::Value = self.json("/fence", Some(json!({"term":term}))).await?;
        Ok(())
    }
    pub async fn handle(&self, request: Request<Body>) -> Response {
        match self.forward(request).await {
            Ok(response) => response,
            Err(_) => (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error":"etcd Raft unavailable"})),
            )
                .into_response(),
        }
    }
    async fn forward(&self, request: Request<Body>) -> anyhow::Result<Response> {
        anyhow::ensure!(!self.cancel.is_cancelled(), "Raft helper stopped");
        if !runner_core::auth::equal_secret(
            request
                .headers()
                .get("x-cp-token")
                .and_then(|v| v.to_str().ok())
                .unwrap_or(""),
            &self.config.secret,
        ) {
            return Ok((
                StatusCode::UNAUTHORIZED,
                Json(json!({"error":"unauthorized"})),
            )
                .into_response());
        }
        let path = match request.uri().path() {
            "/_cp/raft-status" | "/status" => "/status",
            "/_cp/raft" | "/message" => "/message",
            "/_cp/bootstrap" | "/bootstrap" => "/bootstrap",
            "/_cp/join" | "/join" => "/join",
            "/_cp/pull" | "/pull" => "/pull",
            "/_cp/transfer" | "/transfer" => "/transfer",
            "/verify" => "/verify",
            _ => {
                return Ok((
                    StatusCode::NOT_FOUND,
                    Json(json!({"error":"unknown Raft endpoint"})),
                )
                    .into_response());
            }
        };
        if path == "/join" {
            self.verify().await?;
        }
        let query = request
            .uri()
            .query()
            .map(|q| format!("?{q}"))
            .unwrap_or_default();
        let mut send = self
            .client
            .request(
                request.method().clone(),
                format!("{}{path}{query}", self.origin),
            )
            .header("x-cp-token", &self.config.secret);
        for key in [
            "x-cp-cluster",
            "x-cp-generation",
            "x-cp-boot",
            "x-cp-from",
            "content-type",
        ] {
            if let Some(value) = request.headers().get(key) {
                send = send.header(key, value);
            }
        }
        let bytes = to_bytes(request.into_body(), 1 << 20).await?;
        let response = send.body(bytes).send().await?;
        let status = response.status();
        let bytes = runner_core::response_bytes(response, 4 << 20).await?;
        if status == StatusCode::NO_CONTENT {
            return Ok(status.into_response());
        }
        let value = serde_json::from_slice::<serde_json::Value>(&bytes)
            .unwrap_or_else(|_| json!({"error":String::from_utf8_lossy(&bytes)}));
        Ok((status, Json(value)).into_response())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn helper_exit_stops_the_controller_instead_of_preserving_cached_authority()
    -> anyhow::Result<()> {
        let exiting = ["/usr/bin/false", "/bin/false"]
            .into_iter()
            .find(|path| std::path::Path::new(path).is_file())
            .unwrap();
        let env=std::collections::BTreeMap::from([
            ("CP_MEMBERS".into(),json!([{ "id":1,"url":"http://127.0.0.1:1"},{"id":2,"url":"http://127.0.0.1:2"},{"id":3,"url":"http://127.0.0.1:3"}]).to_string()),
            ("CP_NODE_ID".into(),"1".into()),("CP_CLUSTER_ID".into(),"supervisor-failure-test".into()),
            ("CP_CLUSTER_TOKEN".into(),"supervisor-test-secret-at-least-32-characters".into()),
            ("CP_ETCD_RAFT_BINARY".into(),exiting.into()),
        ]);
        let cancel = CancellationToken::new();
        let service = RaftService::start(Config::from_map(&env)?, cancel.clone())?;
        tokio::time::timeout(Duration::from_secs(2), cancel.cancelled()).await?;
        assert!(!service.view().identity_valid);
        assert!(service.verify().await.is_err());
        Ok(())
    }
}
