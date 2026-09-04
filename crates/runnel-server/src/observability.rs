use std::fmt::Write as _;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Json, Router};
use runnel_engine::{BrokerError, Engine, PollResult};
use runnel_protocol::Request;
use runnel_raft::{GroupManager, SnapshotMetricsSnapshot};
use tracing::error;

use crate::protocol::ProtocolAdmission;

const HEALTH_CHECK_TIMEOUT: Duration = Duration::from_secs(1);

#[derive(serde::Serialize)]
struct HealthBody {
    status: &'static str,
    streams: usize,
    storage_bytes: u64,
}

#[derive(Clone)]
struct HttpState {
    engine: Arc<dyn Engine>,
    cluster: Option<Arc<GroupManager>>,
    metrics: Arc<ServerMetrics>,
    admission: ProtocolAdmission,
    shutting_down: Arc<AtomicBool>,
}

pub(crate) fn router(
    engine: Arc<dyn Engine>,
    cluster: Option<Arc<GroupManager>>,
    server_metrics: Arc<ServerMetrics>,
    admission: ProtocolAdmission,
    shutting_down: Arc<AtomicBool>,
) -> Router {
    Router::new()
        .route("/health/live", get(liveness))
        .route("/health/ready", get(readiness))
        .route("/metrics", get(metrics))
        .with_state(HttpState {
            engine,
            cluster,
            metrics: server_metrics,
            admission,
            shutting_down,
        })
}

pub(crate) struct ServerMetrics {
    pub(crate) active_connections: AtomicU64,
    pub(crate) connections_accepted: AtomicU64,
    pub(crate) connections_rejected: AtomicU64,
    pub(crate) connections_closed: AtomicU64,
    pub(crate) connection_errors: AtomicU64,
    pub(crate) active_requests: AtomicU64,
    requests_rejected: AtomicU64,
    pub(crate) request_size_rejections: AtomicU64,
    pub(crate) request_saturation_rejections: AtomicU64,
    pub(crate) request_timeouts: AtomicU64,
    pub(crate) response_write_timeouts: AtomicU64,
    pub(crate) request_bytes: AtomicU64,
    pub(crate) response_bytes: AtomicU64,
    requests: [AtomicU64; REQUEST_OPERATION_COUNT],
    request_failures: [AtomicU64; REQUEST_OPERATION_COUNT],
    request_durations: [RequestDuration; REQUEST_OPERATION_COUNT],
    pub(crate) stream_creations: AtomicU64,
    pub(crate) publishes: AtomicU64,
    pub(crate) published_bytes: AtomicU64,
    deliveries: AtomicU64,
    delivered_bytes: AtomicU64,
    pub(crate) acknowledgements: AtomicU64,
    metrics_scrapes: AtomicU64,
    metrics_scrape_failures: AtomicU64,
    health_check_failures: AtomicU64,
}

const REQUEST_OPERATION_COUNT: usize = 9;
const LATENCY_BUCKET_MICROS: [u64; 6] = [100, 1_000, 10_000, 100_000, 1_000_000, 10_000_000];
const LATENCY_BUCKET_LABELS: [&str; LATENCY_BUCKET_MICROS.len()] =
    ["0.0001", "0.001", "0.01", "0.1", "1", "10"];

#[derive(Clone, Copy)]
pub(crate) enum RequestOperation {
    CreateStream,
    Publish,
    Poll,
    Replay,
    PollGroup,
    Ack,
    AckGroup,
    Health,
    InvalidRequest,
}

impl RequestOperation {
    const ALL: [Self; REQUEST_OPERATION_COUNT] = [
        Self::CreateStream,
        Self::Publish,
        Self::Poll,
        Self::Replay,
        Self::PollGroup,
        Self::Ack,
        Self::AckGroup,
        Self::Health,
        Self::InvalidRequest,
    ];

    const fn index(self) -> usize {
        match self {
            Self::CreateStream => 0,
            Self::Publish => 1,
            Self::Poll => 2,
            Self::Replay => 3,
            Self::PollGroup => 4,
            Self::Ack => 5,
            Self::AckGroup => 6,
            Self::Health => 7,
            Self::InvalidRequest => 8,
        }
    }

    const fn name(self) -> &'static str {
        match self {
            Self::CreateStream => "create_stream",
            Self::Publish => "publish",
            Self::Poll => "poll",
            Self::Replay => "replay",
            Self::PollGroup => "poll_group",
            Self::Ack => "ack",
            Self::AckGroup => "ack_group",
            Self::Health => "health",
            Self::InvalidRequest => "invalid_request",
        }
    }

    pub(crate) fn from_request(request: &Request) -> Self {
        match request {
            Request::CreateStream { .. } => Self::CreateStream,
            Request::Publish { .. } => Self::Publish,
            Request::PublishBytes { .. } => Self::Publish,
            Request::PublishBatch { .. } => Self::Publish,
            Request::Poll { .. } => Self::Poll,
            Request::Replay { .. } => Self::Replay,
            Request::PollGroup { .. } => Self::PollGroup,
            Request::Ack { .. } => Self::Ack,
            Request::AckGroup { .. } => Self::AckGroup,
            Request::Health => Self::Health,
        }
    }
}

struct RequestDuration {
    buckets: [AtomicU64; LATENCY_BUCKET_MICROS.len()],
    count: AtomicU64,
    sum_micros: AtomicU64,
}

impl Default for RequestDuration {
    fn default() -> Self {
        Self {
            buckets: std::array::from_fn(|_| AtomicU64::new(0)),
            count: AtomicU64::new(0),
            sum_micros: AtomicU64::new(0),
        }
    }
}

impl Default for ServerMetrics {
    fn default() -> Self {
        Self {
            active_connections: AtomicU64::new(0),
            connections_accepted: AtomicU64::new(0),
            connections_rejected: AtomicU64::new(0),
            connections_closed: AtomicU64::new(0),
            connection_errors: AtomicU64::new(0),
            active_requests: AtomicU64::new(0),
            requests_rejected: AtomicU64::new(0),
            request_size_rejections: AtomicU64::new(0),
            request_saturation_rejections: AtomicU64::new(0),
            request_timeouts: AtomicU64::new(0),
            response_write_timeouts: AtomicU64::new(0),
            request_bytes: AtomicU64::new(0),
            response_bytes: AtomicU64::new(0),
            requests: std::array::from_fn(|_| AtomicU64::new(0)),
            request_failures: std::array::from_fn(|_| AtomicU64::new(0)),
            request_durations: std::array::from_fn(|_| RequestDuration::default()),
            stream_creations: AtomicU64::new(0),
            publishes: AtomicU64::new(0),
            published_bytes: AtomicU64::new(0),
            deliveries: AtomicU64::new(0),
            delivered_bytes: AtomicU64::new(0),
            acknowledgements: AtomicU64::new(0),
            metrics_scrapes: AtomicU64::new(0),
            metrics_scrape_failures: AtomicU64::new(0),
            health_check_failures: AtomicU64::new(0),
        }
    }
}

impl ServerMetrics {
    pub(crate) fn record_request(
        &self,
        operation: RequestOperation,
        elapsed: Duration,
        failed: bool,
    ) {
        let index = operation.index();
        self.requests[index].fetch_add(1, Ordering::Relaxed);
        if failed {
            self.request_failures[index].fetch_add(1, Ordering::Relaxed);
        }

        let duration = &self.request_durations[index];
        let elapsed_micros = elapsed.as_micros().min(u64::MAX as u128) as u64;
        duration.count.fetch_add(1, Ordering::Relaxed);
        duration
            .sum_micros
            .fetch_add(elapsed_micros, Ordering::Relaxed);
        if let Some(bucket) = LATENCY_BUCKET_MICROS
            .iter()
            .position(|limit| elapsed_micros <= *limit)
        {
            duration.buckets[bucket].fetch_add(1, Ordering::Relaxed);
        }
    }

    pub(crate) fn record_rejected_request(&self, operation: RequestOperation, elapsed: Duration) {
        self.requests_rejected.fetch_add(1, Ordering::Relaxed);
        self.record_request(operation, elapsed, true);
    }
}

pub(crate) struct ActiveConnection(Arc<ServerMetrics>);

impl ActiveConnection {
    pub(crate) fn new(metrics: Arc<ServerMetrics>) -> Self {
        Self(metrics)
    }
}

impl Drop for ActiveConnection {
    fn drop(&mut self) {
        self.0.active_connections.fetch_sub(1, Ordering::Relaxed);
        self.0.connections_closed.fetch_add(1, Ordering::Relaxed);
    }
}

pub(crate) struct ActiveRequest(Arc<ServerMetrics>);

impl ActiveRequest {
    pub(crate) fn new(metrics: Arc<ServerMetrics>) -> Self {
        Self(metrics)
    }
}

impl Drop for ActiveRequest {
    fn drop(&mut self) {
        self.0.active_requests.fetch_sub(1, Ordering::Relaxed);
    }
}

pub(crate) fn record_delivery(metrics: &ServerMetrics, result: &Result<PollResult, BrokerError>) {
    if let Ok(PollResult::Message(message)) = result {
        metrics.deliveries.fetch_add(1, Ordering::Relaxed);
        metrics
            .delivered_bytes
            .fetch_add(message.payload.len() as u64, Ordering::Relaxed);
    }
}

async fn liveness() -> StatusCode {
    StatusCode::OK
}

async fn bounded_health(
    engine: &Arc<dyn Engine>,
) -> Result<runnel_engine::HealthSnapshot, BrokerError> {
    tokio::time::timeout(HEALTH_CHECK_TIMEOUT, engine.health())
        .await
        .map_err(|_| BrokerError::Cluster("health check timed out".to_owned()))?
}

async fn readiness(State(state): State<HttpState>) -> (StatusCode, Json<HealthBody>) {
    if state.shutting_down.load(Ordering::Acquire) {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(HealthBody {
                status: "not_ready",
                streams: 0,
                storage_bytes: 0,
            }),
        );
    }

    match bounded_health(&state.engine).await {
        Ok(health) => (
            StatusCode::OK,
            Json(HealthBody {
                status: "ready",
                streams: health.streams,
                storage_bytes: health.storage_bytes,
            }),
        ),
        Err(error) => {
            state
                .metrics
                .health_check_failures
                .fetch_add(1, Ordering::Relaxed);
            error!(%error, "readiness check failed");
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(HealthBody {
                    status: "not_ready",
                    streams: 0,
                    storage_bytes: 0,
                }),
            )
        }
    }
}

async fn metrics(State(state): State<HttpState>) -> (StatusCode, String) {
    state
        .metrics
        .metrics_scrapes
        .fetch_add(1, Ordering::Relaxed);
    match bounded_health(&state.engine).await {
        Ok(health) => {
            let snapshot_metrics = match &state.cluster {
                Some(cluster) => cluster.snapshot_metrics().await,
                None => SnapshotMetricsSnapshot::default(),
            };
            (
                StatusCode::OK,
                format_metrics(
                    Some(health),
                    Some(snapshot_metrics),
                    &state.metrics,
                    state.admission,
                ),
            )
        }
        Err(error) => {
            state
                .metrics
                .metrics_scrape_failures
                .fetch_add(1, Ordering::Relaxed);
            state
                .metrics
                .health_check_failures
                .fetch_add(1, Ordering::Relaxed);
            error!(%error, "metrics check failed");
            // Keep process and admission telemetry scrapeable without turning unavailable
            // engine-derived samples into fresh-looking zeroes or stale values.
            (
                StatusCode::OK,
                format_metrics(None, None, &state.metrics, state.admission),
            )
        }
    }
}

fn format_metrics(
    health: Option<runnel_engine::HealthSnapshot>,
    snapshot_metrics: Option<SnapshotMetricsSnapshot>,
    metrics: &ServerMetrics,
    admission: ProtocolAdmission,
) -> String {
    let mut output = String::with_capacity(5_000);
    if let Some(health) = health {
        writeln!(
            output,
            "# HELP runnel_streams Number of streams currently known to the broker."
        )
        .unwrap();
        writeln!(
            output,
            "# TYPE runnel_streams gauge\nrunnel_streams {}",
            health.streams
        )
        .unwrap();
        writeln!(
            output,
            "# HELP runnel_storage_bytes Bytes currently occupied by broker storage."
        )
        .unwrap();
        writeln!(
            output,
            "# TYPE runnel_storage_bytes gauge\nrunnel_storage_bytes {}",
            health.storage_bytes
        )
        .unwrap();
        writeln!(
            output,
            "# HELP runnel_in_flight_deliveries Messages currently tracked as delivered but not yet acknowledged."
        )
        .unwrap();
        writeln!(
            output,
            "# TYPE runnel_in_flight_deliveries gauge\nrunnel_in_flight_deliveries {}",
            health.in_flight_deliveries
        )
        .unwrap();
    }
    writeln!(
        output,
        "# HELP runnel_engine_health_available Whether this scrape includes a fresh bounded engine health snapshot."
    )
    .unwrap();
    writeln!(
        output,
        "# TYPE runnel_engine_health_available gauge\nrunnel_engine_health_available {}",
        u8::from(health.is_some())
    )
    .unwrap();
    writeln!(
        output,
        "# HELP runnel_active_connections Broker protocol connections currently being served."
    )
    .unwrap();
    writeln!(
        output,
        "# TYPE runnel_active_connections gauge\nrunnel_active_connections {}",
        metrics.active_connections.load(Ordering::Relaxed)
    )
    .unwrap();
    writeln!(
        output,
        "# HELP runnel_broker_max_connections Configured maximum number of broker protocol connections."
    )
    .unwrap();
    writeln!(
        output,
        "# TYPE runnel_broker_max_connections gauge\nrunnel_broker_max_connections {}",
        admission.max_connections
    )
    .unwrap();
    writeln!(
        output,
        "# HELP runnel_broker_connections_accepted_total Broker protocol connections accepted."
    )
    .unwrap();
    writeln!(
        output,
        "# TYPE runnel_broker_connections_accepted_total counter\nrunnel_broker_connections_accepted_total {}",
        metrics.connections_accepted.load(Ordering::Relaxed)
    )
    .unwrap();
    writeln!(
        output,
        "# HELP runnel_broker_connections_rejected_total Broker protocol connections rejected because the connection limit was reached."
    )
    .unwrap();
    writeln!(
        output,
        "# TYPE runnel_broker_connections_rejected_total counter\nrunnel_broker_connections_rejected_total {}",
        metrics.connections_rejected.load(Ordering::Relaxed)
    )
    .unwrap();
    writeln!(
        output,
        "# HELP runnel_broker_connections_closed_total Broker protocol connections closed."
    )
    .unwrap();
    writeln!(
        output,
        "# TYPE runnel_broker_connections_closed_total counter\nrunnel_broker_connections_closed_total {}",
        metrics.connections_closed.load(Ordering::Relaxed)
    )
    .unwrap();
    writeln!(
        output,
        "# HELP runnel_broker_connection_errors_total Broker protocol connections closed because of a transport or framing error."
    )
    .unwrap();
    writeln!(
        output,
        "# TYPE runnel_broker_connection_errors_total counter\nrunnel_broker_connection_errors_total {}",
        metrics.connection_errors.load(Ordering::Relaxed)
    )
    .unwrap();
    writeln!(
        output,
        "# HELP runnel_active_requests Broker protocol requests executing or writing responses."
    )
    .unwrap();
    writeln!(
        output,
        "# TYPE runnel_active_requests gauge\nrunnel_active_requests {}",
        metrics.active_requests.load(Ordering::Relaxed)
    )
    .unwrap();
    writeln!(
        output,
        "# HELP runnel_broker_max_in_flight_requests Configured maximum number of broker protocol requests executing or writing responses."
    )
    .unwrap();
    writeln!(
        output,
        "# TYPE runnel_broker_max_in_flight_requests gauge\nrunnel_broker_max_in_flight_requests {}",
        admission.max_in_flight_requests
    )
    .unwrap();
    writeln!(
        output,
        "# HELP runnel_broker_max_request_bytes Configured maximum broker protocol request frame size in bytes."
    )
    .unwrap();
    writeln!(
        output,
        "# TYPE runnel_broker_max_request_bytes gauge\nrunnel_broker_max_request_bytes {}",
        admission.max_request_bytes
    )
    .unwrap();
    writeln!(
        output,
        "# HELP runnel_broker_request_timeout_seconds Configured maximum broker protocol request duration in seconds."
    )
    .unwrap();
    writeln!(
        output,
        "# TYPE runnel_broker_request_timeout_seconds gauge\nrunnel_broker_request_timeout_seconds {:.9}",
        admission.request_timeout.as_secs_f64()
    )
    .unwrap();
    writeln!(
        output,
        "# HELP runnel_broker_requests_rejected_total Broker protocol requests rejected before completion, including admission, framing, and timeout failures."
    )
    .unwrap();
    writeln!(
        output,
        "# TYPE runnel_broker_requests_rejected_total counter\nrunnel_broker_requests_rejected_total {}",
        metrics.requests_rejected.load(Ordering::Relaxed)
    )
    .unwrap();
    writeln!(
        output,
        "# HELP runnel_broker_request_size_rejections_total Broker protocol requests rejected because their frame exceeded the configured maximum."
    )
    .unwrap();
    writeln!(
        output,
        "# TYPE runnel_broker_request_size_rejections_total counter\nrunnel_broker_request_size_rejections_total {}",
        metrics.request_size_rejections.load(Ordering::Relaxed)
    )
    .unwrap();
    writeln!(
        output,
        "# HELP runnel_broker_request_saturation_total Broker protocol requests rejected because all in-flight request permits were occupied."
    )
    .unwrap();
    writeln!(
        output,
        "# TYPE runnel_broker_request_saturation_total counter\nrunnel_broker_request_saturation_total {}",
        metrics
            .request_saturation_rejections
            .load(Ordering::Relaxed)
    )
    .unwrap();
    writeln!(
        output,
        "# HELP runnel_broker_request_timeouts_total Broker protocol requests whose frame or engine work exceeded its timeout."
    )
    .unwrap();
    writeln!(
        output,
        "# HELP runnel_broker_response_write_timeouts_total Broker protocol responses whose socket write exceeded the request timeout."
    )
    .unwrap();
    writeln!(
        output,
        "# TYPE runnel_broker_response_write_timeouts_total counter\nrunnel_broker_response_write_timeouts_total {}",
        metrics.response_write_timeouts.load(Ordering::Relaxed)
    )
    .unwrap();
    writeln!(
        output,
        "# TYPE runnel_broker_request_timeouts_total counter\nrunnel_broker_request_timeouts_total {}",
        metrics.request_timeouts.load(Ordering::Relaxed)
    )
    .unwrap();
    writeln!(
        output,
        "# HELP runnel_broker_request_bytes_total Bytes received in broker protocol request lines, including line delimiters."
    )
    .unwrap();
    writeln!(
        output,
        "# TYPE runnel_broker_request_bytes_total counter\nrunnel_broker_request_bytes_total {}",
        metrics.request_bytes.load(Ordering::Relaxed)
    )
    .unwrap();
    writeln!(
        output,
        "# HELP runnel_broker_response_bytes_total Bytes written in broker protocol responses, including line delimiters."
    )
    .unwrap();
    writeln!(
        output,
        "# TYPE runnel_broker_response_bytes_total counter\nrunnel_broker_response_bytes_total {}",
        metrics.response_bytes.load(Ordering::Relaxed)
    )
    .unwrap();
    if let Some(health) = health {
        writeln!(
            output,
            "# HELP runnel_redeliveries_total Messages delivered again after an acknowledgement timeout."
        )
        .unwrap();
        writeln!(
            output,
            "# TYPE runnel_redeliveries_total counter\nrunnel_redeliveries_total {}",
            health.redeliveries
        )
        .unwrap();
        writeln!(
            output,
            "# HELP runnel_dead_letters_total Messages moved to a dead-letter stream after reaching the delivery limit."
        )
        .unwrap();
        writeln!(
            output,
            "# TYPE runnel_dead_letters_total counter\nrunnel_dead_letters_total {}",
            health.dead_letters
        )
        .unwrap();
    }
    writeln!(
        output,
        "# HELP runnel_deliveries_total Messages returned by successful poll operations."
    )
    .unwrap();
    writeln!(
        output,
        "# TYPE runnel_deliveries_total counter\nrunnel_deliveries_total {}",
        metrics.deliveries.load(Ordering::Relaxed)
    )
    .unwrap();
    writeln!(
        output,
        "# HELP runnel_delivered_bytes_total Payload bytes returned by successful poll operations."
    )
    .unwrap();
    writeln!(
        output,
        "# TYPE runnel_delivered_bytes_total counter\nrunnel_delivered_bytes_total {}",
        metrics.delivered_bytes.load(Ordering::Relaxed)
    )
    .unwrap();
    writeln!(
        output,
        "# HELP runnel_stream_creations_total Successful stream creation requests."
    )
    .unwrap();
    writeln!(
        output,
        "# TYPE runnel_stream_creations_total counter\nrunnel_stream_creations_total {}",
        metrics.stream_creations.load(Ordering::Relaxed)
    )
    .unwrap();
    writeln!(
        output,
        "# HELP runnel_publishes_total Messages durably accepted by publish requests."
    )
    .unwrap();
    writeln!(
        output,
        "# TYPE runnel_publishes_total counter\nrunnel_publishes_total {}",
        metrics.publishes.load(Ordering::Relaxed)
    )
    .unwrap();
    writeln!(
        output,
        "# HELP runnel_published_bytes_total Payload bytes durably accepted by publish requests."
    )
    .unwrap();
    writeln!(
        output,
        "# TYPE runnel_published_bytes_total counter\nrunnel_published_bytes_total {}",
        metrics.published_bytes.load(Ordering::Relaxed)
    )
    .unwrap();
    writeln!(
        output,
        "# HELP runnel_acknowledgements_total Successful acknowledgement requests."
    )
    .unwrap();
    writeln!(
        output,
        "# TYPE runnel_acknowledgements_total counter\nrunnel_acknowledgements_total {}",
        metrics.acknowledgements.load(Ordering::Relaxed)
    )
    .unwrap();
    writeln!(
        output,
        "# HELP runnel_metrics_scrapes_total Metrics endpoint requests."
    )
    .unwrap();
    writeln!(
        output,
        "# TYPE runnel_metrics_scrapes_total counter\nrunnel_metrics_scrapes_total {}",
        metrics.metrics_scrapes.load(Ordering::Relaxed)
    )
    .unwrap();
    writeln!(
        output,
        "# HELP runnel_metrics_scrape_failures_total Metrics endpoint requests that could not complete a bounded health check."
    )
    .unwrap();
    writeln!(
        output,
        "# TYPE runnel_metrics_scrape_failures_total counter\nrunnel_metrics_scrape_failures_total {}",
        metrics.metrics_scrape_failures.load(Ordering::Relaxed)
    )
    .unwrap();
    writeln!(
        output,
        "# HELP runnel_health_check_failures_total Readiness or metrics health checks that failed or timed out."
    )
    .unwrap();
    writeln!(
        output,
        "# TYPE runnel_health_check_failures_total counter\nrunnel_health_check_failures_total {}",
        metrics.health_check_failures.load(Ordering::Relaxed)
    )
    .unwrap();
    writeln!(
        output,
        "# HELP runnel_broker_requests_total Broker protocol requests by fixed operation kind."
    )
    .unwrap();
    writeln!(output, "# TYPE runnel_broker_requests_total counter").unwrap();
    for operation in RequestOperation::ALL {
        let index = operation.index();
        writeln!(
            output,
            "runnel_broker_requests_total{{operation=\"{}\"}} {}",
            operation.name(),
            metrics.requests[index].load(Ordering::Relaxed)
        )
        .unwrap();
    }
    writeln!(
        output,
        "# HELP runnel_broker_request_failures_total Broker protocol requests that returned an error by fixed operation kind."
    )
    .unwrap();
    writeln!(
        output,
        "# TYPE runnel_broker_request_failures_total counter"
    )
    .unwrap();
    for operation in RequestOperation::ALL {
        let index = operation.index();
        writeln!(
            output,
            "runnel_broker_request_failures_total{{operation=\"{}\"}} {}",
            operation.name(),
            metrics.request_failures[index].load(Ordering::Relaxed)
        )
        .unwrap();
    }
    writeln!(
        output,
        "# HELP runnel_broker_request_duration_seconds Broker protocol request duration by fixed operation kind."
    )
    .unwrap();
    writeln!(
        output,
        "# TYPE runnel_broker_request_duration_seconds histogram"
    )
    .unwrap();
    for operation in RequestOperation::ALL {
        let duration = &metrics.request_durations[operation.index()];
        let mut cumulative = 0;
        for (index, label) in LATENCY_BUCKET_LABELS.iter().enumerate() {
            cumulative += duration.buckets[index].load(Ordering::Relaxed);
            writeln!(
                output,
                "runnel_broker_request_duration_seconds_bucket{{operation=\"{}\",le=\"{}\"}} {}",
                operation.name(),
                label,
                cumulative
            )
            .unwrap();
        }
        writeln!(
            output,
            "runnel_broker_request_duration_seconds_bucket{{operation=\"{}\",le=\"+Inf\"}} {}",
            operation.name(),
            duration.count.load(Ordering::Relaxed)
        )
        .unwrap();
        writeln!(
            output,
            "runnel_broker_request_duration_seconds_sum{{operation=\"{}\"}} {:.6}",
            operation.name(),
            duration.sum_micros.load(Ordering::Relaxed) as f64 / 1_000_000.0
        )
        .unwrap();
        writeln!(
            output,
            "runnel_broker_request_duration_seconds_count{{operation=\"{}\"}} {}",
            operation.name(),
            duration.count.load(Ordering::Relaxed)
        )
        .unwrap();
    }
    if let Some(snapshot_metrics) = snapshot_metrics {
        writeln!(output, "# TYPE runnel_snapshot_builds_started_total counter\nrunnel_snapshot_builds_started_total {}", snapshot_metrics.builds_started).unwrap();
        writeln!(output, "# TYPE runnel_snapshot_builds_completed_total counter\nrunnel_snapshot_builds_completed_total {}", snapshot_metrics.builds_completed).unwrap();
        writeln!(output, "# TYPE runnel_snapshot_build_failures_total counter\nrunnel_snapshot_build_failures_total {}", snapshot_metrics.build_failures).unwrap();
        writeln!(output, "# TYPE runnel_snapshot_installs_started_total counter\nrunnel_snapshot_installs_started_total {}", snapshot_metrics.installs_started).unwrap();
        writeln!(output, "# TYPE runnel_snapshot_installs_completed_total counter\nrunnel_snapshot_installs_completed_total {}", snapshot_metrics.installs_completed).unwrap();
        writeln!(output, "# TYPE runnel_snapshot_install_failures_total counter\nrunnel_snapshot_install_failures_total {}", snapshot_metrics.install_failures).unwrap();
        writeln!(output, "# TYPE runnel_snapshot_install_bytes_total counter\nrunnel_snapshot_install_bytes_total {}", snapshot_metrics.install_bytes).unwrap();
        writeln!(output, "# TYPE runnel_snapshot_installs_in_progress gauge\nrunnel_snapshot_installs_in_progress {}", snapshot_metrics.installs_in_progress).unwrap();
        writeln!(output, "# TYPE runnel_snapshot_transfer_chunks_received_total counter\nrunnel_snapshot_transfer_chunks_received_total {}", snapshot_metrics.transfer_chunks).unwrap();
        writeln!(output, "# TYPE runnel_snapshot_transfer_final_chunks_received_total counter\nrunnel_snapshot_transfer_final_chunks_received_total {}", snapshot_metrics.transfer_final_chunks).unwrap();
        writeln!(output, "# TYPE runnel_snapshot_transfer_bytes_received_total counter\nrunnel_snapshot_transfer_bytes_received_total {}", snapshot_metrics.transfer_bytes).unwrap();
    }
    output
}
