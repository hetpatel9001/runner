use axum::{
    extract::Request,
    middleware::{self, Next},
    response::Response,
};
use runner_raft::{
    config::Config,
    model::{Identity, Manifest},
    service::RaftService,
};
use std::{collections::BTreeMap, time::Duration};
use tokio_util::sync::CancellationToken;

async fn delayed(request: Request, next: Next) -> Response {
    tokio::time::sleep(Duration::from_millis(200)).await;
    let response = next.run(request).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    response
}

#[tokio::test(flavor = "current_thread")]
async fn verification_burst_retains_quorum_over_delayed_links() -> anyhow::Result<()> {
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
    let members = listeners.iter().enumerate().map(|(i,l)| serde_json::json!({"id":i+1,"url":format!("http://{}",l.local_addr().unwrap())})).collect::<Vec<_>>();
    let secret = "test-verification-load-secret-32-characters";
    let mut services = Vec::new();
    let mut stops = Vec::new();
    for (i, listener) in listeners.into_iter().enumerate() {
        let env = BTreeMap::from([
            ("CP_MEMBERS".into(), serde_json::to_string(&members)?),
            ("CP_NODE_ID".into(), (i + 1).to_string()),
            ("CP_CLUSTER_ID".into(), "verification-load-test".into()),
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
        services.push(raft);
    }
    tokio::time::timeout(Duration::from_secs(2), async {
        while services.iter().any(|r| r.view().id == 0) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    let manifest = Manifest {
        generation: "isolated-verification-test".into(),
        term: 1000,
        members: services
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
        let code = response.status();
        assert!(
            code.is_success(),
            "bootstrap: {code} {}",
            response.text().await?
        );
    }
    let leader = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if let Some(r) = services.iter().find(|r| r.view().role == "StateLeader") {
                if r.verify().await.is_ok() {
                    break r.clone();
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await?;
    let term = leader.view().term;
    let mut checks = tokio::task::JoinSet::new();
    for _ in 0..64 {
        let raft = leader.clone();
        checks.spawn(async move { raft.verify().await });
    }
    let mut failures = 0;
    while let Some(result) = checks.join_next().await {
        if result?.is_err() {
            failures += 1;
        }
    }
    assert_eq!(
        failures, 0,
        "fresh quorum checks timed out during a normal request burst"
    );
    assert_eq!(leader.view().term, term, "request load caused an election");
    assert!(
        leader.verify().await.is_ok(),
        "quorum did not remain usable after the burst"
    );
    let mut ongoing = tokio::task::JoinSet::new();
    for n in 0..64 {
        let raft = leader.clone();
        ongoing.spawn(async move {
            tokio::time::sleep(Duration::from_millis(n * 35)).await;
            raft.verify().await
        });
    }
    let mut failures = 0;
    while let Some(result) = ongoing.join_next().await {
        if result?.is_err() {
            failures += 1;
        }
    }
    assert_eq!(failures, 0, "continuing checks saturated the peer queues");
    // A later caller must not borrow a confirmation already sent before a
    // partition. Old acknowledgements can still arrive after the cut.
    let before_cut = leader.clone();
    let in_flight = tokio::spawn(async move { before_cut.verify().await });
    tokio::time::sleep(Duration::from_millis(250)).await;
    assert!(
        !in_flight.is_finished(),
        "delayed round finished before the cut"
    );
    for (raft, stop) in services
        .iter()
        .zip(&stops)
        .filter(|(r, _)| r.view().id != leader.view().id)
        .take(4)
    {
        assert!(
            raft.verify().await.is_err(),
            "a follower confirmed authority"
        );
        stop.cancel();
    }
    assert!(
        leader.verify().await.is_err(),
        "batching confirmed authority after quorum loss"
    );
    let _ = in_flight.await?;
    Ok(())
}
