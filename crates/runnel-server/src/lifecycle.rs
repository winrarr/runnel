use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use axum::Router;
use runnel_engine::Engine;
use runnel_raft::GroupManager;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tracing::{error, info, warn};

use crate::connection;
use crate::observability::{self, ServerMetrics};
use crate::protocol::ProtocolAdmission;

const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(25);

pub(crate) async fn run(
    tcp_listener: TcpListener,
    http_listener: TcpListener,
    engine: Arc<dyn Engine>,
    peer: Option<(TcpListener, Arc<GroupManager>)>,
    cluster: Option<Arc<GroupManager>>,
    server_metrics: Arc<ServerMetrics>,
    protocol_admission: ProtocolAdmission,
) -> Result<(), Box<dyn std::error::Error>> {
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let shutting_down = Arc::new(AtomicBool::new(false));
    let mut tcp_task = connection::spawn(
        tcp_listener,
        Arc::clone(&engine),
        Arc::clone(&server_metrics),
        protocol_admission,
        shutdown_rx.clone(),
    );
    let mut peer_task = spawn_peer(peer, shutdown_rx.clone());

    let app = observability::router(
        engine,
        cluster,
        server_metrics,
        protocol_admission,
        Arc::clone(&shutting_down),
    );
    let mut http_task = tokio::spawn(serve_http(http_listener, app, shutdown_rx));

    let shutdown_reason = tokio::select! {
        result = &mut tcp_task => {
            result??;
            "broker listener stopped"
        }
        result = &mut http_task => {
            result??;
            "http listener stopped"
        }
        result = shutdown_signal() => {
            result?;
            "shutdown signal received"
        }
    };

    shutting_down.store(true, Ordering::Release);
    let _ = shutdown_tx.send(true);
    info!(reason = shutdown_reason, "runnel shutting down");

    let drain = async {
        let _ = (&mut tcp_task).await;
        let _ = (&mut http_task).await;
        if let Some(peer_task) = peer_task.as_mut() {
            let _ = peer_task.await;
        }
    };
    if tokio::time::timeout(SHUTDOWN_TIMEOUT, drain).await.is_err() {
        warn!(?SHUTDOWN_TIMEOUT, "runnel shutdown drain timed out");
        tcp_task.abort();
        http_task.abort();
        if let Some(peer_task) = peer_task {
            peer_task.abort();
        }
    }
    Ok(())
}

fn spawn_peer(
    peer: Option<(TcpListener, Arc<GroupManager>)>,
    shutdown: watch::Receiver<bool>,
) -> Option<tokio::task::JoinHandle<()>> {
    peer.map(|(peer_listener, group)| {
        tokio::spawn(async move {
            if let Err(error) = runnel_raft::serve_peer(peer_listener, group, shutdown).await {
                error!(%error, "raft peer listener stopped");
            }
        })
    })
}

async fn serve_http(
    listener: TcpListener,
    app: Router,
    shutdown: watch::Receiver<bool>,
) -> Result<(), std::io::Error> {
    axum::serve(listener, app)
        .with_graceful_shutdown(wait_for_shutdown(shutdown))
        .await
}

async fn wait_for_shutdown(mut shutdown: watch::Receiver<bool>) {
    let _ = shutdown.wait_for(|value| *value).await;
}

#[cfg(unix)]
async fn shutdown_signal() -> Result<(), std::io::Error> {
    use tokio::signal::unix::{SignalKind, signal};

    let mut terminate = signal(SignalKind::terminate())?;
    tokio::select! {
        result = tokio::signal::ctrl_c() => result,
        _ = terminate.recv() => Ok(()),
    }
}

#[cfg(not(unix))]
async fn shutdown_signal() -> Result<(), std::io::Error> {
    tokio::signal::ctrl_c().await
}
