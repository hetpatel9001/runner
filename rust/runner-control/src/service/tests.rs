use super::*;
use crate::etcd::{
    Config, RaftService,
    model::{Identity, Manifest},
};
use anyhow::Context;
use axum::{
    extract::Request as HttpRequest,
    middleware::{self, Next},
    response::Response,
};
use std::{collections::BTreeMap, time::Instant};

async fn delayed(request: HttpRequest, next: Next) -> Response {
    tokio::time::sleep(Duration::from_millis(100)).await;
    let response = next.run(request).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    response
}

fn request(index: usize) -> Request {
    let sync = index % 12 == 0;
    let body = if sync {
        let now = runner_core::now_ms();
        serde_json::to_vec(&json!({"run":format!("load-{index}"),"agent":format!("load-{index}"),"machine":index+1,"started":now-3600000,"protocol":5,"ready":true,"starts":true,"tag":"test","status":{},"inventory":{},"cpus":4,"metrics":{"live":{"t":now,"h":{"cpu":12,"memUsed":1000,"memTotal":8000},"a":{}},"minutes":[]}})).unwrap().into()
    } else {
        bytes::Bytes::new()
    };
    let mut headers = axum::http::HeaderMap::new();
    headers.insert("authorization", "Bearer test-join".parse().unwrap());
    Request {
        method: if sync {
            axum::http::Method::POST
        } else {
            axum::http::Method::GET
        },
        path: if sync {
            "/api/sync"
        } else if index % 8 == 7 {
            "/api/metrics"
        } else {
            "/admin/api/status"
        }
        .into(),
        query: BTreeMap::new(),
        headers,
        origin: "http://127.0.0.1".into(),
        ip: "127.0.0.1".into(),
        body,
        permit: None,
    }
}

#[tokio::test(flavor = "current_thread")]
async fn report_load_does_not_stall_overview_or_scheduler_ticks_and_quorum_loss_still_blocks_plans()
-> anyhow::Result<()> {
    let cancel = CancellationToken::new();
    struct Stop(CancellationToken);
    impl Drop for Stop {
        fn drop(&mut self) {
            self.0.cancel();
        }
    }
    let _stop = Stop(cancel.clone());
    let mut listeners = Vec::new();
    for _ in 0..7 {
        listeners.push(tokio::net::TcpListener::bind("127.0.0.1:0").await?);
    }
    let members = listeners
        .iter()
        .enumerate()
        .map(|(i, l)| json!({"id":i+1,"url":format!("http://{}",l.local_addr().unwrap())}))
        .collect::<Vec<_>>();
    let secret = "overview-load-test-secret-at-least-32-characters";
    let mut rafts = Vec::new();
    let mut stops = Vec::new();
    for (i, listener) in listeners.into_iter().enumerate() {
        let env = BTreeMap::from([
            ("CP_MEMBERS".into(), serde_json::to_string(&members)?),
            ("CP_NODE_ID".into(), (i + 1).to_string()),
            ("CP_CLUSTER_ID".into(), "overview-load-test".into()),
            ("CP_CLUSTER_TOKEN".into(), secret.into()),
            ("CP_RAFT_TICK_MS".into(), "100".into()),
            ("CP_RAFT_HEARTBEAT_TICKS".into(), "5".into()),
            ("CP_RAFT_ELECTION_TICKS".into(), "50".into()),
        ]);
        let stop = cancel.child_token();
        stops.push(stop.clone());
        let raft = RaftService::start(Config::from_map(&env)?, stop.clone())?;
        let router = raft.router().layer(middleware::from_fn(delayed));
        tokio::spawn(async move {
            let _ = axum::serve(listener, router)
                .with_graceful_shutdown(stop.cancelled_owned())
                .await;
        });
        rafts.push(raft);
    }
    tokio::time::timeout(Duration::from_secs(2), async {
        while rafts.iter().any(|r| r.view().id == 0) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    let manifest = Manifest {
        generation: "overview-load-generation".into(),
        term: 1000,
        members: rafts
            .iter()
            .map(|r| {
                let v = r.view();
                Identity {
                    id: v.id,
                    node: v.node,
                    boot: v.boot.clone(),
                }
            })
            .collect(),
    };
    let client = reqwest::Client::new();
    for member in &members {
        let response = client
            .post(format!("{}/_cp/bootstrap", member["url"].as_str().unwrap()))
            .header("x-cp-token", secret)
            .json(&manifest)
            .send()
            .await?;
        assert!(response.status().is_success());
    }
    let raft = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if let Some(r) = rafts.iter().find(|r| r.view().role == "StateLeader") {
                if r.verify().await.is_ok() {
                    break r.clone();
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await?;
    let authority = Authority::new(
        raft.clone(),
        "overview-load-test".into(),
        raft.view().id,
        false,
    );
    let proof = authority.renew().await?;
    let renew = authority.clone();
    let renew_cancel = cancel.clone();
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(2));
        loop {
            tokio::select! { _=renew_cancel.cancelled()=>break,_=tick.tick()=>{let _=renew.renew().await;} }
        }
    });
    let env = BTreeMap::from([
        ("DOMAIN".into(), "test.example".into()),
        ("CONTROL_HOST".into(), "runners.test.example".into()),
        ("JOIN_TOKEN".into(), "test-join".into()),
        ("STATE_SIGNING_KEY".into(), "test-signing-key".into()),
        ("DNS".into(), "off".into()),
        ("AUTO_TUNNEL".into(), "0".into()),
        ("RECOVER_TUNNELS".into(), "0".into()),
        ("RECOVER_RUNTIME".into(), "0".into()),
        ("REQUIRE_DEPLOY_AUTH".into(), "0".into()),
        ("QUIET_MS".into(), "0".into()),
        ("POLL_S".into(), "20".into()),
    ]);
    // Only the service uses distributed authority here. The fixture's local
    // SQLite writes are not D1 configuration transactions (the native HA
    // matrix covers the latter's additional immediate commit guard).
    let mut control = Control::new(Store::memory()?, env, runner_core::now_ms())?;
    control.tunnels_ready = true;
    let (commands, receiver) = mpsc::channel(32);
    let (ready_tx, ready) = watch::channel(false);
    let service = Service {
        commands,
        hub: Hub::new(cancel.clone()),
        cancel: cancel.child_token(),
        ready,
        authority: Some((authority, proof.clone())),
    };
    let mut replies = Vec::new();
    for index in 0..24 {
        let (reply, receive) = oneshot::channel();
        service
            .commands
            .try_send(Command::Api(request(index), reply))
            .map_err(|_| anyhow::anyhow!("test queue full"))?;
        replies.push(receive);
    }
    let actor = service.clone();
    let actor_cancel = service.cancel.clone();
    let tail = Arc::new(Mutex::new(Tail::default()));
    let actor_task = tokio::spawn(async move {
        let result = actor.run(control, tail, receiver, ready_tx).await;
        actor_cancel.cancel();
        result
    });
    let started = Instant::now();
    let mut ready = service.ready.clone();
    tokio::time::timeout(Duration::from_secs(3), ready.wait_for(|value| *value))
        .await
        .context("scheduler tick was starved by queued API requests")??;
    let outputs = tokio::time::timeout(
        Duration::from_secs(5),
        futures_util::future::join_all(replies),
    )
    .await
    .context("Overview reads were stalled behind per-request quorum checks")?;
    let mut plans = 0;
    eprintln!(
        "load fixture completed 24 responses in {} ms",
        started.elapsed().as_millis()
    );
    for output in outputs {
        match output?? {
            Output::Json(value) if value.get("leadership").is_some() => {
                assert_eq!(value["leadership"], json!(proof));
                plans += 1;
            }
            Output::Json(_) | Output::Bytes(_) => (),
            _ => panic!("unexpected API response"),
        }
    }
    assert_eq!(
        plans, 2,
        "agent reports must still receive quorum-authorized plans"
    );
    assert!(started.elapsed() < Duration::from_secs(6));
    for (r, stop) in rafts
        .iter()
        .zip(stops)
        .filter(|(r, _)| r.view().id != raft.view().id)
        .take(4)
    {
        let _ = r;
        stop.cancel();
    }
    let responses = (0..8)
        .map(|index| service.request(request(index * 12)))
        .collect::<Vec<_>>();
    let results = tokio::time::timeout(
        Duration::from_secs(8),
        futures_util::future::join_all(responses),
    )
    .await?;
    assert!(
        results.iter().all(Result::is_err),
        "a lost quorum must not release plans from a request batch"
    );
    assert!(actor_task.await?.is_err());
    Ok(())
}
