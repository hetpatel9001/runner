use crate::{error::http, node::Node};
use axum::{body::Body, response::Response};
use futures_util::{StreamExt, TryStreamExt, stream};
use serde::Deserialize;
use std::{
    io::SeekFrom,
    path::{Component, Path},
};
use tokio::io::{AsyncReadExt, AsyncSeekExt};

#[derive(Deserialize)]
struct Part {
    file: String,
    offset: u64,
    size: u64,
}

async fn artifact(
    root: &Path,
    relative: &str,
) -> anyhow::Result<(Vec<(tokio::fs::File, u64, u64)>, u64)> {
    match tokio::fs::File::open(root.join(relative)).await {
        Ok(file) => {
            let size = file.metadata().await?.len();
            Ok((vec![(file, 0, size)], size))
        }
        Err(error)
            if error.kind() == std::io::ErrorKind::NotFound && relative == "rust/bundle.tar" =>
        {
            let index = tokio::fs::File::open(root.join("rust/bundle.index.json"))
                .await
                .map_err(|_| http(404, "agent asset unavailable"))?;
            anyhow::ensure!(
                index.metadata().await?.len() <= 16 << 10,
                "invalid agent archive index"
            );
            let mut bytes = Vec::new();
            index.take(16 << 10).read_to_end(&mut bytes).await?;
            let index: Vec<Part> = serde_json::from_slice(&bytes)?;
            anyhow::ensure!(
                !index.is_empty() && index.len() <= 8,
                "invalid agent archive parts"
            );
            let mut parts = Vec::new();
            let mut size = 0_u64;
            for part in index {
                anyhow::ensure!(
                    matches!(
                        part.file.as_str(),
                        "bundle.parts" | "runner-agent-linux-amd64" | "runner-agent-linux-arm64"
                    ),
                    "invalid agent archive file"
                );
                let file = tokio::fs::File::open(root.join("rust").join(part.file)).await?;
                let file_size = file.metadata().await?.len();
                anyhow::ensure!(
                    part.offset
                        .checked_add(part.size)
                        .is_some_and(|end| end <= file_size),
                    "invalid agent archive range"
                );
                size = size
                    .checked_add(part.size)
                    .filter(|size| *size <= 64 << 20)
                    .ok_or_else(|| http(500, "agent archive too large"))?;
                parts.push((file, part.offset, part.size));
            }
            Ok((parts, size))
        }
        Err(_) => Err(http(404, "agent asset unavailable")),
    }
}
pub async fn serve(node: &Node, path: &str) -> anyhow::Result<Response> {
    if path == "/install.sh" {
        let quote = |s: &str| format!("'{}'", s.replace('\'', "'\\''"));
        let origin = if node.e("PUBLIC_URL").is_empty() {
            format!("https://{}", node.e("CONTROL_HOST"))
        } else {
            node.e("PUBLIC_URL").into()
        };
        let agent = if node.e("AGENT_URL").is_empty() {
            format!("{origin}/agent")
        } else {
            node.e("AGENT_URL").into()
        };
        let script = include_str!("../../install-agent.sh")
            .replace("@ORIGIN@", &quote(&origin))
            .replace("@AGENT_URL@", &quote(&agent));
        return Ok(Response::builder()
            .status(200)
            .header("content-type", "text/plain; charset=utf-8")
            .header("cache-control", "no-store")
            .body(Body::from(script))?);
    }
    let relative = if path == "/install.sh" {
        "install.sh"
    } else {
        path.strip_prefix("/agent/")
            .ok_or_else(|| http(404, "not found"))?
    };
    let allowed = relative == "install.sh"
        || relative.ends_with(".mjs")
            && relative
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || matches!(b, b'-' | b'.'))
        || [
            "bundle.json",
            "manifest.json",
            "bundle.tar",
            "bundle.sha256",
            "rust/bundle.tar",
            "rust/bundle.sha256",
            "rust/manifest.json",
        ]
        .contains(&relative)
        || relative.strip_prefix("rust/").is_some_and(|p| {
            p.starts_with("runner-agent-linux-")
                && p.bytes()
                    .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.'))
        });
    if !allowed
        || !Path::new(relative)
            .components()
            .all(|c| matches!(c, Component::Normal(_)))
    {
        return Err(http(404, "agent asset unavailable"));
    }
    let root = if node.e("AGENT_ASSET_DIR").is_empty() {
        "/usr/local/share/runner-agent"
    } else {
        node.e("AGENT_ASSET_DIR")
    };
    let (parts, size) = artifact(Path::new(root), relative).await?;
    anyhow::ensure!(size <= 64 << 20, "agent artifact too large");
    let body = stream::iter(parts)
        .then(|(mut file, offset, size)| async move {
            if offset > 0 {
                file.seek(SeekFrom::Start(offset)).await?;
            }
            Ok::<_, std::io::Error>(tokio_util::io::ReaderStream::with_capacity(
                file.take(size),
                16 << 10,
            ))
        })
        .try_flatten();
    let content = if relative.ends_with(".json") {
        "application/json"
    } else if relative.ends_with(".mjs") {
        "text/javascript; charset=utf-8"
    } else if relative.ends_with(".tar") {
        "application/x-tar"
    } else if relative.ends_with(".sh") || relative.ends_with(".sha256") {
        "text/plain; charset=utf-8"
    } else {
        "application/octet-stream"
    };
    Ok(Response::builder()
        .status(200)
        .header("content-type", content)
        .header("content-length", size)
        .header("cache-control", "no-cache")
        .body(Body::from_stream(body))?)
}
