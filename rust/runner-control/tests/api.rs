use reqwest::Method;
const GET: Method = Method::GET;
const POST: Method = Method::POST;
const PUT: Method = Method::PUT;
use futures_util::{SinkExt, StreamExt};
use runner_control::{control::Control, node::Node, server, service::Service, storage::Store};
use serde_json::{Value, json};
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream,
    tungstenite::{Message, client::IntoClientRequest},
};
use tokio_util::sync::CancellationToken;
type Socket = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;
struct Fixture {
    base: String,
    node: Arc<Node>,
    client: reqwest::Client,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.node.cancel.cancel();
        self.task.abort();
    }
}
impl Fixture {
    async fn new() -> anyhow::Result<Self> {
        Self::with_metrics_limit(None).await
    }
    async fn with_metrics_limit(limit: Option<u64>) -> anyhow::Result<Self> {
        Self::with_options(limit, false).await
    }
    async fn with_options(limit: Option<u64>, recovering: bool) -> anyhow::Result<Self> {
        let mut env = BTreeMap::from(
            [
                ("DOMAIN", "test.example"),
                ("CONTROL_HOST", "runners.test.example"),
                ("JOIN_TOKEN", "test-join"),
                ("STATE_SIGNING_KEY", "test-signing-key"),
                ("ADMIN_PASSWORD", "test-admin"),
                ("DNS", "off"),
                ("AUTO_TUNNEL", "0"),
                ("RECOVER_TUNNELS", "0"),
                ("RECOVER_RUNTIME", "0"),
                ("REQUIRE_DEPLOY_AUTH", "0"),
                ("TRUSTED_PROXIES", ""),
                ("QUIET_MS", "0"),
                ("POLL_S", "20"),
            ]
            .map(|(k, v)| (k.into(), v.into())),
        );
        if let Some(limit) = limit {
            env.insert("METRICS_QUERY_BYTES".into(), limit.to_string());
        }
        if recovering {
            env.insert("RECOVER_RUNTIME".into(), "1".into());
            env.insert("RECOVERY_MS".into(), "60000".into());
        }
        let cancel = CancellationToken::new();
        let node = Node::new(env.clone(), None, cancel.clone())?;
        let mut c = Control::new(Store::memory()?, env, runner_core::now_ms())?;
        if limit.is_some() {
            let time = runner_core::now_ms() / 60_000 * 60_000 - 180_000;
            c.store.exec(
                "INSERT INTO metrics_1m(t,data) VALUES(?,?)",
                &[
                    json!(time),
                    json!(
                        json!({"1":{"h":{"cpu":12,"memUsed":1234,"memTotal":8000},"a":{}}})
                            .to_string()
                    ),
                ],
            )?;
        }
        node.current
            .send_replace(Some(Service::from_control(c, cancel.child_token())));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let base = format!("http://{}", listener.local_addr()?);
        let app = server::router(node.clone());
        let task = tokio::spawn(async move {
            axum::serve(
                listener,
                app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
            )
            .await
            .unwrap();
        });
        Ok(Self {
            base,
            node,
            client: runner_core::http_client(Duration::from_secs(10))?,
            task,
        })
    }
    fn req(&self, method: reqwest::Method, path: &str) -> reqwest::RequestBuilder {
        self.client.request(method, format!("{}{path}", self.base))
    }
    async fn json(
        &self,
        method: reqwest::Method,
        path: &str,
        body: Value,
        headers: &[(&str, &str)],
    ) -> anyhow::Result<(u16, Value)> {
        let mut request = self.req(method, path).json(&body);
        for (k, v) in headers {
            request = request.header(*k, *v);
        }
        let response = request.send().await?;
        Ok((response.status().as_u16(), response.json().await?))
    }
    async fn socket(&self, path: &str, agent: bool) -> anyhow::Result<Socket> {
        let mut request =
            format!("{}{path}", self.base.replacen("http", "ws", 1)).into_client_request()?;
        request.headers_mut().insert("origin", self.base.parse()?);
        if agent {
            request
                .headers_mut()
                .insert("authorization", "Bearer test-join".parse()?);
        }
        Ok(tokio_tungstenite::connect_async(request).await?.0)
    }
}
#[tokio::test(flavor = "current_thread")]
async fn dashboard_reads_remain_available_when_report_body_slots_are_full() -> anyhow::Result<()> {
    let f = Fixture::new().await?;
    let held = f.node.bodies.clone().acquire_many_owned(4).await?;
    assert_eq!(f.req(GET, "/api/status").send().await?.status(), 200);
    assert_eq!(
        f.req(POST, "/api/join").body("{}").send().await?.status(),
        503,
        "report bodies must remain bounded"
    );
    assert_eq!(
        f.req(GET, "/api/status").body("{}").send().await?.status(),
        503,
        "GET bodies must remain bounded too"
    );
    drop(held);
    assert_eq!(f.req(GET, "/api/status").send().await?.status(), 200);
    Ok(())
}
#[tokio::test(flavor = "current_thread")]
async fn recovery_retries_do_not_restart_the_primary_or_its_deadline() -> anyhow::Result<()> {
    let f = Fixture::with_options(None, true).await?;
    let initial: Value = f.req(GET, "/api/status").send().await?.json().await?;
    let deadline = initial["controlPlane"]["recoveryUntil"].as_u64().unwrap();
    assert!(deadline > runner_core::now_ms());
    for _ in 0..3 {
        let (status, body) = f
            .json(
                POST,
                "/api/join",
                json!({"agent":"recovery-test", "tag":"test"}),
                &[("authorization", "Bearer test-join")],
            )
            .await?;
        assert_eq!(status, 503);
        assert_eq!(
            body["error"],
            "recovering fleet inventory; retry joining shortly"
        );
        assert_eq!(f.req(GET, "/readyz").send().await?.status(), 200);
        let after: Value = f.req(GET, "/api/status").send().await?.json().await?;
        assert_eq!(after["controlPlane"]["recoveryUntil"], deadline);
    }
    assert_eq!(f.json(PUT, "/api/projects/web", spec(), &[]).await?.0, 200);
    Ok(())
}
#[tokio::test(flavor = "current_thread")]
async fn oversized_metrics_queries_do_not_stop_the_primary() -> anyhow::Result<()> {
    let f = Fixture::with_metrics_limit(Some(512)).await?;
    let response = f.req(GET, "/api/metrics?range=1h").send().await?;
    assert_eq!(response.status(), 413);
    assert!(response.text().await?.contains("shorter range"));
    assert_eq!(f.req(GET, "/api/status").send().await?.status(), 200);
    assert_eq!(f.json(PUT, "/api/projects/web", spec(), &[]).await?.0, 200);
    Ok(())
}
fn spec() -> Value {
    json!({"compose":"services:\n  app:\n    image: nginx\n    ports: [\"8080:80\"]\n","port":8080,"replicas":1})
}
static START: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
fn report(run: &str, machine: u64, status: Value) -> Value {
    let now = runner_core::now_ms();
    json!({"run":run,"agent":run,"machine":machine,"started":*START.get_or_init(||now-3600000),"protocol":5,"ready":true,"starts":true,"tag":"test","status":status,"inventory":{},"cpus":4,"metrics":{"live":{"t":now,"h":{"cpu":12,"memUsed":1000,"memTotal":8000},"a":{}},"minutes":[]}})
}
async fn receive(socket: &mut Socket) -> anyhow::Result<Value> {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match socket.next().await {
                Some(Ok(Message::Text(text))) => return Ok(serde_json::from_str(&text)?),
                Some(Ok(Message::Ping(data))) => socket.send(Message::Pong(data)).await?,
                other => anyhow::bail!("unexpected socket result: {other:?}"),
            }
        }
    })
    .await?
}

#[tokio::test(flavor = "current_thread")]
async fn authentication_matches_existing_ui_and_script_boundaries() -> anyhow::Result<()> {
    let f = Fixture::new().await?;
    assert_eq!(f.req(GET, "/api/status").send().await?.status(), 200);
    assert_eq!(
        f.json(
            POST,
            "/admin/api/join",
            json!({"agent":"x"}),
            &[("x-admin-password", "test-admin")]
        )
        .await?
        .0,
        401,
        "UI route never grants machine powers"
    );
    assert_eq!(
        f.json(POST, "/api/sync", report("a", 1, json!({})), &[])
            .await?
            .0,
        401
    );
    assert_eq!(f.req(GET, "/api/join-token").send().await?.status(), 401);
    assert_eq!(
        f.json(
            GET,
            "/api/join-token",
            Value::Null,
            &[("x-fleet-password", "test-admin")]
        )
        .await?
        .1["token"],
        "test-join"
    );
    assert_eq!(
        f.json(
            PUT,
            "/admin/api/projects/web",
            spec(),
            &[("origin", "https://other.example")]
        )
        .await?
        .0,
        403
    );
    for i in 0..5 {
        assert_eq!(
            f.json(
                POST,
                "/admin/api/unlock",
                json!({}),
                &[
                    ("x-admin-password", "wrong"),
                    ("cf-connecting-ip", &format!("198.51.100.{i}")),
                    ("x-runner-client-ip", "forged")
                ]
            )
            .await?
            .0,
            401
        );
    }
    assert_eq!(
        f.json(
            POST,
            "/admin/api/unlock",
            json!({}),
            &[("x-admin-password", "test-admin")]
        )
        .await?
        .0,
        429,
        "untrusted client IP headers cannot evade lockout"
    );
    assert_eq!(
        f.req(GET, "/admin/api/status").send().await?.status(),
        200,
        "read-only public views remain available"
    );
    let mut input = spec();
    input["password"] = json!("application-password");
    assert_eq!(
        f.json(PUT, "/api/projects/locked", input, &[]).await?.0,
        200
    );
    assert_eq!(
        f.json(
            POST,
            "/admin/api/projects/locked/unlock",
            json!({}),
            &[("x-app-password", "application-password")]
        )
        .await?
        .0,
        200,
        "an app's password has a separate lockout bucket"
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn app_mutations_are_atomic_and_private_details_stay_authenticated() -> anyhow::Result<()> {
    let f = Fixture::new().await?;
    let mut input = spec();
    input["password"] = json!("application-password");
    input["env"] = json!({"TOKEN":"private-app-secret"});
    let (code, created) = f.json(PUT, "/api/projects/web", input, &[]).await?;
    assert_eq!(code, 200);
    assert_eq!(created["hasPassword"], true);
    assert_eq!(
        f.json(POST, "/api/projects/web/disable", json!({}), &[])
            .await?
            .0,
        401
    );
    let (code, env) = f
        .json(GET, "/api/projects/web/env", Value::Null, &[])
        .await?;
    assert_eq!(code, 200);
    assert_eq!(env["keys"], json!(["TOKEN"]));
    assert!(!env.to_string().contains("private-app-secret"));
    let (code, _) = f
        .json(
            PUT,
            "/api/projects/web/env",
            json!({"TOKEN":"new-private-secret","BAD-NAME":"no"}),
            &[("x-app-password", "application-password")],
        )
        .await?;
    assert_eq!(code, 400);
    let (_, same) = f
        .json(
            PUT,
            "/api/projects/web/env",
            json!({"TOKEN":"private-app-secret"}),
            &[("x-app-password", "application-password")],
        )
        .await?;
    assert_eq!(same["unchanged"], true);
    assert_eq!(same["version"], 1);
    let agent = [("authorization", "Bearer test-join")];
    assert_eq!(
        f.json(POST, "/api/sync", report("a", 1, json!({})), &agent)
            .await?
            .0,
        200
    );
    f.json(
        POST,
        "/api/sync",
        report(
            "a",
            1,
            json!({"web":{"v":1,"s":"failed","e":"failure with private-app-secret and test-join"}}),
        ),
        &agent,
    )
    .await?;
    let status: Value = f.req(GET, "/api/status").send().await?.json().await?;
    assert!(!status.to_string().contains("private-app-secret"));
    assert!(!status.to_string().contains("test-join"));
    assert_eq!(
        f.json(POST, "/api/projects/web/diagnostics", json!({}), &[])
            .await?
            .0,
        401
    );
    let (_, diagnostics) = f
        .json(
            POST,
            "/api/projects/web/diagnostics",
            json!({}),
            &[("x-app-password", "application-password")],
        )
        .await?;
    assert!(diagnostics.to_string().contains("[hidden]"));
    assert!(!diagnostics.to_string().contains("private-app-secret"));
    let import = json!({"table":"settings","rows":[{"key":"rebalance","value":"off"},{}]});
    assert_eq!(
        f.json(
            POST,
            "/api/import",
            import,
            &[("x-admin-password", "test-admin")]
        )
        .await?
        .0,
        400
    );
    let status: Value = f.req(GET, "/api/status").send().await?.json().await?;
    assert_eq!(
        status["rebalance"]["on"], true,
        "failed import must roll back SQL and in-memory maps"
    );
    assert_eq!(
        f.req(POST, "/api/sync")
            .header("authorization", "Bearer test-join")
            .body("x".repeat(1_000_001))
            .send()
            .await?
            .status(),
        413
    );
    assert_eq!(f.req(GET, "/api/status").send().await?.status(), 200);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn websocket_reconnects_wakes_and_log_sessions_have_bounded_lifetimes() -> anyhow::Result<()>
{
    let f = Fixture::new().await?;
    assert!(f.socket("/api/agent", false).await.is_err());
    let mut first = f.socket("/api/agent", true).await?;
    first
        .send(Message::Text(
            json!({"type":"sync","id":1,"body":report("a",1,json!({}))})
                .to_string()
                .into(),
        ))
        .await?;
    assert_eq!(receive(&mut first).await?["type"], "plan");
    let mut input = spec();
    input["env"] = json!({"TOKEN":"private-app-secret"});
    f.json(PUT, "/api/projects/web", input, &[]).await?;
    let mut second = f.socket("/api/agent", true).await?;
    second
        .send(Message::Text(
            json!({"type":"sync","id":2,"body":report("a",1,json!({"web":{"v":1,"s":"healthy"}}))})
                .to_string()
                .into(),
        ))
        .await?;
    let plan = receive(&mut second).await?;
    assert_eq!(plan["type"], "plan");
    assert_eq!(plan["id"], 2);
    assert!(
        tokio::time::timeout(Duration::from_secs(3), first.next())
            .await?
            .is_none_or(|m| matches!(m, Ok(Message::Close(_)) | Err(_)))
    );
    assert_eq!(
        f.json(POST, "/api/projects/web/logs", json!({}), &[])
            .await?
            .0,
        401
    );
    let (_, log) = f
        .json(
            POST,
            "/api/projects/web/logs",
            json!({}),
            &[("x-admin-password", "test-admin")],
        )
        .await?;
    let id = log["session"].as_str().unwrap();
    let mut viewer = f.socket(&format!("/admin/api/logs/{id}"), false).await?;
    assert_eq!(receive(&mut viewer).await?["t"], "info");
    assert_eq!(receive(&mut second).await?["type"], "wake");
    assert!(
        f.socket(
            &format!("/api/logs/stream?session={id}&run=unrelated"),
            true
        )
        .await
        .is_err()
    );
    let mut logs = f
        .socket(&format!("/api/logs/stream?session={id}&run=a"), true)
        .await?;
    assert_eq!(receive(&mut viewer).await?["t"], "open");
    logs.send(Message::Text("line containing private-app-secret".into()))
        .await?;
    let line = receive(&mut viewer).await?;
    assert_eq!(line["text"], "line containing [hidden]");
    viewer.close(None).await?;
    assert!(
        tokio::time::timeout(Duration::from_secs(3), logs.next())
            .await?
            .is_none_or(|m| matches!(m, Ok(Message::Close(_)) | Err(_)))
    );
    second
        .send(Message::Text(
            json!({"type":"sync","id":3,"body":report("different-run",1,json!({}))})
                .to_string()
                .into(),
        ))
        .await?;
    assert!(
        tokio::time::timeout(Duration::from_secs(3), second.next())
            .await?
            .is_none_or(|m| matches!(m, Ok(Message::Close(_)) | Err(_)))
    );
    Ok(())
}

#[tokio::test]
async fn streamed_installer_preserves_exact_bytes_and_native_bootstrap_files() -> anyhow::Result<()>
{
    let assets = tempfile::tempdir()?;
    let native = assets.path().join("rust");
    std::fs::create_dir(&native)?;
    let expected = (0..32_769).map(|n| (n % 251) as u8).collect::<Vec<_>>();
    std::fs::write(native.join("runner-agent-linux-arm64"), &expected)?;
    let mut metadata = vec![0_u8; 512];
    metadata.extend_from_slice(b"archive-footer");
    metadata.extend_from_slice(b"must-not-be-part-of-the-download");
    std::fs::write(native.join("bundle.parts"), &metadata)?;
    let mut archive = metadata[..512].to_vec();
    archive.extend_from_slice(&expected);
    archive.extend_from_slice(b"archive-footer");
    let index = native.join("bundle.index.json");
    std::fs::write(
        &index,
        serde_json::to_vec(&json!([
            {"file":"bundle.parts","offset":0,"size":512},
            {"file":"runner-agent-linux-arm64","offset":0,"size":expected.len()},
            {"file":"bundle.parts","offset":512,"size":14}
        ]))?,
    )?;
    let node = Node::new(
        BTreeMap::from([(
            "AGENT_ASSET_DIR".into(),
            assets.path().to_str().unwrap().into(),
        )]),
        None,
        CancellationToken::new(),
    )?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let base = format!("http://{}", listener.local_addr()?);
    let app = server::router(node.clone());
    let task = tokio::spawn(async move {
        let _ = axum::serve(
            listener,
            app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
        )
        .await;
    });
    let fixture = Fixture {
        base,
        node,
        client: reqwest::Client::new(),
        task,
    };
    let url = format!("{}/agent/rust/runner-agent-linux-arm64", fixture.base);
    let response = fixture.client.get(&url).send().await?;
    assert_eq!(response.status(), 200);
    assert_eq!(response.content_length(), Some(expected.len() as u64));
    assert_eq!(
        response.headers()["content-type"],
        "application/octet-stream"
    );
    assert_eq!(&response.bytes().await?[..], &expected);
    let archive_url = format!("{}/agent/rust/bundle.tar", fixture.base);
    let response = fixture.client.get(&archive_url).send().await?;
    assert_eq!(response.status(), 200);
    assert_eq!(response.content_length(), Some(archive.len() as u64));
    assert_eq!(response.headers()["content-type"], "application/x-tar");
    assert_eq!(&response.bytes().await?[..], &archive);
    for private in ["bundle.index.json", "bundle.parts"] {
        assert_eq!(
            fixture
                .client
                .get(format!("{}/agent/rust/{private}", fixture.base))
                .send()
                .await?
                .status(),
            404
        );
    }
    assert_eq!(
        fixture
            .client
            .get(format!(
                "{}/agent/rust/runner-agent-linux-unknown",
                fixture.base
            ))
            .send()
            .await?
            .status(),
        404
    );
    std::fs::write(
        &index,
        serde_json::to_vec(&json!([
            {"file":"bundle.parts","offset":u64::MAX,"size":17}
        ]))?,
    )?;
    assert_eq!(
        fixture.client.get(&archive_url).send().await?.status(),
        500,
        "an invalid index must not serve adjacent bytes"
    );
    std::fs::write(
        &index,
        serde_json::to_vec(&json!([
            {"file":"../../outside","offset":0,"size":17}
        ]))?,
    )?;
    assert_eq!(fixture.client.get(&archive_url).send().await?.status(), 500);
    // Existing externally supplied archives remain supported, without an index.
    std::fs::write(native.join("bundle.tar"), &archive)?;
    assert_eq!(
        &fixture
            .client
            .get(&archive_url)
            .send()
            .await?
            .bytes()
            .await?[..],
        &archive
    );
    Ok(())
}
