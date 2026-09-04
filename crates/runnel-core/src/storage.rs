use std::collections::{HashMap, VecDeque};
use std::io;
use std::sync::{Arc, Mutex, Weak};

#[cfg(feature = "instrumentation")]
use runnel_engine::StageTimer;
use runnel_engine::{BrokerError, EngineFuture};
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};

use crate::validate_name;

const DEFAULT_STORAGE_EXECUTOR_CAPACITY: usize = 32;
const DEFAULT_STORAGE_QUEUE_CAPACITY: usize = 32;

pub(super) struct StorageExecutor {
    execution_permits: Arc<Semaphore>,
    admission_permits: Arc<Semaphore>,
    // This bounds async tasks waiting for a busy stream across all per-stream lanes.
    stream_waiter_permits: Arc<Semaphore>,
    // Weak entries keep transient lane state from growing with one-off requests for unknown
    // streams. Active and queued operations hold the strong lane reference until they finish.
    stream_lanes: Mutex<HashMap<String, Weak<StorageLane>>>,
    // This bounds work waiting for a busy stream without charging it against global admission.
    stream_queue_capacity: usize,
}

struct StorageLane {
    state: Mutex<StorageLaneState>,
    queue_capacity: usize,
}

struct StorageLaneState {
    ownership: LaneOwnership,
    waiters: VecDeque<Arc<StorageLaneWaiter>>,
}

enum LaneOwnership {
    // No operation owns the lane and no handoff is pending.
    Idle,
    // The current operation owns the lane.
    Held,
    // The current operation released the lane and the front waiter must claim it.
    Handoff,
}

struct StorageLaneWaiter {
    notify: Notify,
}

struct QueuedStorageLaneWaiter {
    lane: Arc<StorageLane>,
    waiter: Arc<StorageLaneWaiter>,
    waiter_permit: Option<OwnedSemaphorePermit>,
    claimed: bool,
}

struct StorageLanePermit {
    lane: Arc<StorageLane>,
}

impl StorageExecutor {
    pub(super) fn new() -> Self {
        Self::with_capacities(
            DEFAULT_STORAGE_EXECUTOR_CAPACITY,
            DEFAULT_STORAGE_QUEUE_CAPACITY,
        )
    }

    fn with_capacities(execution_capacity: usize, queue_capacity: usize) -> Self {
        Self {
            execution_permits: Arc::new(Semaphore::new(execution_capacity)),
            admission_permits: Arc::new(Semaphore::new(
                execution_capacity.saturating_add(queue_capacity),
            )),
            stream_waiter_permits: Arc::new(Semaphore::new(queue_capacity)),
            stream_lanes: Mutex::new(HashMap::new()),
            stream_queue_capacity: queue_capacity,
        }
    }

    pub(super) fn dispatch<T, F>(self: Arc<Self>, operation: F) -> EngineFuture<'static, T>
    where
        T: Send + 'static,
        F: FnOnce() -> Result<T, BrokerError> + Send + 'static,
    {
        self.dispatch_with_lane(None, operation)
    }

    pub(super) fn dispatch_stream<T, F>(
        self: Arc<Self>,
        stream: String,
        operation: F,
    ) -> EngineFuture<'static, T>
    where
        T: Send + 'static,
        F: FnOnce(&str) -> Result<T, BrokerError> + Send + 'static,
    {
        if let Err(error) = validate_name("stream", &stream) {
            return Box::pin(async move { Err(error) });
        }
        let lane = match self.stream_lane(&stream) {
            Ok(lane) => lane,
            Err(error) => return Box::pin(async move { Err(error) }),
        };
        self.dispatch_with_lane(Some(lane), move || operation(&stream))
    }

    fn dispatch_with_lane<T, F>(
        self: Arc<Self>,
        stream_lane: Option<Arc<StorageLane>>,
        operation: F,
    ) -> EngineFuture<'static, T>
    where
        T: Send + 'static,
        F: FnOnce() -> Result<T, BrokerError> + Send + 'static,
    {
        Box::pin(async move {
            #[cfg(feature = "instrumentation")]
            let _stage_timer = StageTimer::new("core.storage_dispatch");

            // Admission is reserved before waiting for an execution permit, so the number of
            // active and queued blocking operations is bounded. Waiting for execution remains
            // asynchronous; only the synchronous operation runs on Tokio's blocking pool. A
            // A stream waiter is kept outside execution admission while it waits for its explicit
            // FIFO lane, but a separate bounded pool caps the total number of such waiters. Each
            // lane has its configured bounded queue, so one stalled stream cannot consume global
            // execution slots with operations that are unable to make progress in stream order.
            let stream_permit = match stream_lane.as_ref() {
                Some(stream_lane) => Some(
                    Self::acquire_stream_permit(
                        Arc::clone(stream_lane),
                        Arc::clone(&self.stream_waiter_permits),
                    )
                    .await?,
                ),
                None => None,
            };
            let admission_permit = Arc::clone(&self.admission_permits)
                .try_acquire_owned()
                .map_err(|_| {
                    BrokerError::Io(io::Error::new(
                        io::ErrorKind::WouldBlock,
                        "storage execution queue is full",
                    ))
                })?;
            let execution_permit = Arc::clone(&self.execution_permits)
                .acquire_owned()
                .await
                .map_err(|_| {
                    BrokerError::Io(io::Error::other("storage execution capacity was closed"))
                })?;
            tokio::task::spawn_blocking(move || {
                let _admission_permit = admission_permit;
                let _stream_lane = stream_lane;
                let _stream_permit = stream_permit;
                let _execution_permit = execution_permit;
                operation()
            })
            .await
            .map_err(|error| {
                let message = if error.is_panic() {
                    "storage execution task panicked"
                } else {
                    "storage execution task was cancelled"
                };
                BrokerError::Io(io::Error::other(message))
            })?
        })
    }

    async fn acquire_stream_permit(
        stream_lane: Arc<StorageLane>,
        stream_waiter_permits: Arc<Semaphore>,
    ) -> Result<StorageLanePermit, BrokerError> {
        let reservation = stream_lane.reserve(stream_waiter_permits)?;
        match reservation {
            LaneReservation::Active(permit) => Ok(permit),
            LaneReservation::Queued(waiter) => waiter.wait().await,
        }
    }

    fn stream_lane(&self, stream: &str) -> Result<Arc<StorageLane>, BrokerError> {
        let mut stream_lanes = self
            .stream_lanes
            .lock()
            .map_err(|_| BrokerError::LockPoisoned)?;
        stream_lanes.retain(|_, lane| lane.upgrade().is_some());
        if let Some(lane) = stream_lanes.get(stream).and_then(Weak::upgrade) {
            return Ok(lane);
        }

        let lane = Arc::new(StorageLane {
            state: Mutex::new(StorageLaneState {
                ownership: LaneOwnership::Idle,
                waiters: VecDeque::new(),
            }),
            queue_capacity: self.stream_queue_capacity,
        });
        stream_lanes.insert(stream.to_owned(), Arc::downgrade(&lane));
        Ok(lane)
    }

    #[cfg(test)]
    pub(super) fn execution_permit_is_consumed(&self) -> bool {
        self.execution_permits.available_permits() < DEFAULT_STORAGE_EXECUTOR_CAPACITY
    }
}

enum LaneReservation {
    Active(StorageLanePermit),
    Queued(QueuedStorageLaneWaiter),
}

impl StorageLane {
    fn reserve(
        self: &Arc<Self>,
        stream_waiter_permits: Arc<Semaphore>,
    ) -> Result<LaneReservation, BrokerError> {
        let mut state = self.state.lock().map_err(|_| BrokerError::LockPoisoned)?;
        if matches!(state.ownership, LaneOwnership::Idle) && state.waiters.is_empty() {
            state.ownership = LaneOwnership::Held;
            return Ok(LaneReservation::Active(StorageLanePermit {
                lane: Arc::clone(self),
            }));
        }
        if state.waiters.len() >= self.queue_capacity {
            return Err(BrokerError::Io(io::Error::new(
                io::ErrorKind::WouldBlock,
                "storage stream queue is full",
            )));
        }
        let waiter_permit = stream_waiter_permits.try_acquire_owned().map_err(|_| {
            BrokerError::Io(io::Error::new(
                io::ErrorKind::WouldBlock,
                "storage stream queue is full",
            ))
        })?;

        let waiter = Arc::new(StorageLaneWaiter {
            notify: Notify::new(),
        });
        state.waiters.push_back(Arc::clone(&waiter));
        Ok(LaneReservation::Queued(QueuedStorageLaneWaiter {
            lane: Arc::clone(self),
            waiter,
            waiter_permit: Some(waiter_permit),
            claimed: false,
        }))
    }

    fn claim(&self, waiter: &Arc<StorageLaneWaiter>) -> Result<bool, BrokerError> {
        let mut state = self.state.lock().map_err(|_| BrokerError::LockPoisoned)?;
        if !matches!(state.ownership, LaneOwnership::Handoff) {
            return Ok(false);
        }
        let is_front = state
            .waiters
            .front()
            .is_some_and(|front| Arc::ptr_eq(front, waiter));
        if !is_front {
            return Ok(false);
        }

        state.waiters.pop_front();
        state.ownership = LaneOwnership::Held;
        Ok(true)
    }

    fn cancel(&self, waiter: &Arc<StorageLaneWaiter>) {
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        let Some(position) = state
            .waiters
            .iter()
            .position(|queued| Arc::ptr_eq(queued, waiter))
        else {
            return;
        };
        state.waiters.remove(position);
        if matches!(state.ownership, LaneOwnership::Handoff) && state.waiters.is_empty() {
            state.ownership = LaneOwnership::Idle;
        }
        if position == 0
            && let Some(next) = state.waiters.front()
        {
            next.notify.notify_one();
        }
    }

    #[cfg(test)]
    fn queue_len(&self) -> Result<usize, BrokerError> {
        Ok(self
            .state
            .lock()
            .map_err(|_| BrokerError::LockPoisoned)?
            .waiters
            .len())
    }

    fn release(&self) {
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        if let Some(next) = state.waiters.front().cloned() {
            state.ownership = LaneOwnership::Handoff;
            next.notify.notify_one();
        } else {
            state.ownership = LaneOwnership::Idle;
        }
    }
}

impl QueuedStorageLaneWaiter {
    async fn wait(mut self) -> Result<StorageLanePermit, BrokerError> {
        loop {
            let notified = self.waiter.notify.notified();
            if self.lane.claim(&self.waiter)? {
                self.claimed = true;
                self.waiter_permit.take();
                return Ok(StorageLanePermit {
                    lane: Arc::clone(&self.lane),
                });
            }
            notified.await;
        }
    }
}

impl Drop for QueuedStorageLaneWaiter {
    fn drop(&mut self) {
        if !self.claimed {
            self.lane.cancel(&self.waiter);
        }
    }
}

impl Drop for StorageLanePermit {
    fn drop(&mut self) {
        self.lane.release();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering as AtomicOrdering};
    use std::thread;

    #[tokio::test(flavor = "current_thread")]
    async fn storage_dispatch_runs_off_the_async_runtime_thread() {
        let runtime_thread = thread::current().id();
        let executor = Arc::new(StorageExecutor::with_capacities(1, 1));
        let storage_thread = executor
            .dispatch(|| Ok(thread::current().id()))
            .await
            .unwrap();

        assert_ne!(storage_thread, runtime_thread);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn storage_dispatch_does_not_block_unrelated_async_work() {
        let executor = Arc::new(StorageExecutor::with_capacities(1, 1));
        let started = Arc::new(tokio::sync::Notify::new());
        let (release_sender, release_receiver) = std::sync::mpsc::channel();
        let storage_started = Arc::clone(&started);
        let storage = tokio::spawn(Arc::clone(&executor).dispatch(move || {
            storage_started.notify_one();
            release_receiver
                .recv()
                .expect("storage task should be released by the test");
            Ok(())
        }));

        started.notified().await;
        let unrelated = tokio::spawn(async { 42_u8 });
        assert_eq!(unrelated.await.unwrap(), 42);

        release_sender.send(()).unwrap();
        storage.await.unwrap().unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn storage_dispatch_waits_when_capacity_is_exhausted() {
        let executor = Arc::new(StorageExecutor::with_capacities(1, 1));
        let started = Arc::new(tokio::sync::Notify::new());
        let (release_sender, release_receiver) = std::sync::mpsc::channel();
        let storage_started = Arc::clone(&started);
        let first = tokio::spawn(Arc::clone(&executor).dispatch(move || {
            storage_started.notify_one();
            release_receiver
                .recv()
                .expect("storage task should be released by the test");
            Ok(())
        }));

        started.notified().await;
        let second = tokio::spawn(executor.dispatch(|| Ok(())));
        tokio::task::yield_now().await;
        assert!(!second.is_finished());

        release_sender.send(()).unwrap();
        first.await.unwrap().unwrap();
        second.await.unwrap().unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn storage_dispatch_rejects_work_beyond_the_bounded_queue() {
        let executor = Arc::new(StorageExecutor::with_capacities(1, 1));
        let started = Arc::new(tokio::sync::Notify::new());
        let (release_sender, release_receiver) = std::sync::mpsc::channel();
        let storage_started = Arc::clone(&started);
        let first = tokio::spawn(Arc::clone(&executor).dispatch(move || {
            storage_started.notify_one();
            release_receiver
                .recv()
                .expect("storage task should be released by the test");
            Ok(())
        }));

        started.notified().await;
        let second = tokio::spawn(Arc::clone(&executor).dispatch(|| Ok(())));
        while executor.admission_permits.available_permits() != 0 {
            tokio::task::yield_now().await;
        }

        let rejected = executor.dispatch(|| Ok(())).await;
        assert!(matches!(
            rejected,
            Err(BrokerError::Io(error)) if error.kind() == io::ErrorKind::WouldBlock
        ));

        release_sender.send(()).unwrap();
        first.await.unwrap().unwrap();
        second.await.unwrap().unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn storage_dispatch_cancellation_drops_queued_work() {
        let executor = Arc::new(StorageExecutor::with_capacities(1, 1));
        let started = Arc::new(tokio::sync::Notify::new());
        let (release_sender, release_receiver) = std::sync::mpsc::channel();
        let storage_started = Arc::clone(&started);
        let first = tokio::spawn(Arc::clone(&executor).dispatch(move || {
            storage_started.notify_one();
            release_receiver
                .recv()
                .expect("storage task should be released by the test");
            Ok(())
        }));

        started.notified().await;
        let executed = Arc::new(AtomicBool::new(false));
        let queued_executed = Arc::clone(&executed);
        let second = tokio::spawn(executor.clone().dispatch(move || {
            queued_executed.store(true, AtomicOrdering::Release);
            Ok(())
        }));
        while executor.admission_permits.available_permits() != 0 {
            tokio::task::yield_now().await;
        }

        second.abort();
        assert!(second.await.unwrap_err().is_cancelled());
        release_sender.send(()).unwrap();
        first.await.unwrap().unwrap();

        assert!(!executed.load(AtomicOrdering::Acquire));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn storage_dispatch_lanes_preserve_stream_order_without_starving_other_streams() {
        let executor = Arc::new(StorageExecutor::with_capacities(2, 2));
        let started = Arc::new(tokio::sync::Notify::new());
        let (release_sender, release_receiver) = std::sync::mpsc::channel();
        let order = Arc::new(Mutex::new(Vec::new()));
        let first_order = Arc::clone(&order);
        let first_started = Arc::clone(&started);
        let first = tokio::spawn(Arc::clone(&executor).dispatch_stream(
            "events".to_owned(),
            move |_| {
                first_order.lock().unwrap().push(1_u8);
                first_started.notify_one();
                release_receiver
                    .recv()
                    .expect("storage task should be released by the test");
                Ok(())
            },
        ));

        started.notified().await;
        let second_order = Arc::clone(&order);
        let second = tokio::spawn(Arc::clone(&executor).dispatch_stream(
            "events".to_owned(),
            move |_| {
                second_order.lock().unwrap().push(2_u8);
                Ok(())
            },
        ));
        let third_order = Arc::clone(&order);
        let third = tokio::spawn(Arc::clone(&executor).dispatch_stream(
            "events".to_owned(),
            move |_| {
                third_order.lock().unwrap().push(3_u8);
                Ok(())
            },
        ));
        let events_lane = executor.stream_lane("events").unwrap();
        while events_lane.queue_len().unwrap() != 2 {
            tokio::task::yield_now().await;
        }

        // The follower waits for the stream lane without consuming a global queue slot.
        assert_eq!(executor.admission_permits.available_permits(), 3);
        assert_eq!(executor.execution_permits.available_permits(), 1);
        assert!(!second.is_finished());
        let rejected = executor
            .clone()
            .dispatch_stream("events".to_owned(), |_| Ok(()))
            .await;
        assert!(matches!(
            rejected,
            Err(BrokerError::Io(error)) if error.kind() == io::ErrorKind::WouldBlock
        ));
        assert_eq!(
            executor
                .clone()
                .dispatch_stream("jobs".to_owned(), |_| Ok(42_u8))
                .await
                .unwrap(),
            42
        );

        release_sender.send(()).unwrap();
        first.await.unwrap().unwrap();
        second.await.unwrap().unwrap();
        third.await.unwrap().unwrap();
        assert_eq!(*order.lock().unwrap(), vec![1, 2, 3]);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn storage_dispatch_bounds_stream_waiters_across_lanes() {
        let executor = Arc::new(StorageExecutor::with_capacities(3, 2));
        let started = Arc::new(AtomicUsize::new(0));
        let release = Arc::new(AtomicBool::new(false));
        let mut active = Vec::new();

        for stream in ["events", "jobs", "metrics"] {
            let started = Arc::clone(&started);
            let release = Arc::clone(&release);
            active.push(tokio::spawn(Arc::clone(&executor).dispatch_stream(
                stream.to_owned(),
                move |_| {
                    started.fetch_add(1, AtomicOrdering::Release);
                    while !release.load(AtomicOrdering::Acquire) {
                        thread::yield_now();
                    }
                    Ok(())
                },
            )));
        }

        while started.load(AtomicOrdering::Acquire) != 3 {
            tokio::task::yield_now().await;
        }

        let first_waiter = tokio::spawn(
            executor
                .clone()
                .dispatch_stream("events".to_owned(), |_| Ok(())),
        );
        let second_waiter = tokio::spawn(
            executor
                .clone()
                .dispatch_stream("jobs".to_owned(), |_| Ok(())),
        );
        while executor.stream_waiter_permits.available_permits() != 0 {
            tokio::task::yield_now().await;
        }

        assert_eq!(executor.admission_permits.available_permits(), 2);
        let rejected = executor
            .clone()
            .dispatch_stream("metrics".to_owned(), |_| Ok(()))
            .await;
        assert!(matches!(
            rejected,
            Err(BrokerError::Io(error)) if error.kind() == io::ErrorKind::WouldBlock
        ));

        release.store(true, AtomicOrdering::Release);
        for operation in active {
            operation.await.unwrap().unwrap();
        }
        first_waiter.await.unwrap().unwrap();
        second_waiter.await.unwrap().unwrap();
        assert_eq!(executor.stream_waiter_permits.available_permits(), 2);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn storage_dispatch_lane_cancellation_releases_the_stream_queue_slot() {
        let executor = Arc::new(StorageExecutor::with_capacities(1, 1));
        let started = Arc::new(tokio::sync::Notify::new());
        let (release_sender, release_receiver) = std::sync::mpsc::channel();
        let first_started = Arc::clone(&started);
        let first = tokio::spawn(Arc::clone(&executor).dispatch_stream(
            "events".to_owned(),
            move |_| {
                first_started.notify_one();
                release_receiver
                    .recv()
                    .expect("storage task should be released by the test");
                Ok(())
            },
        ));

        started.notified().await;
        let second_executed = Arc::new(AtomicBool::new(false));
        let second_executed_flag = Arc::clone(&second_executed);
        let second = tokio::spawn(Arc::clone(&executor).dispatch_stream(
            "events".to_owned(),
            move |_| {
                second_executed_flag.store(true, AtomicOrdering::Release);
                Ok(())
            },
        ));
        let events_lane = executor.stream_lane("events").unwrap();
        while events_lane.queue_len().unwrap() != 1 {
            tokio::task::yield_now().await;
        }

        second.abort();
        assert!(second.await.unwrap_err().is_cancelled());
        assert_eq!(events_lane.queue_len().unwrap(), 0);
        assert_eq!(executor.stream_waiter_permits.available_permits(), 1);

        let third_executed = Arc::new(AtomicBool::new(false));
        let third_executed_flag = Arc::clone(&third_executed);
        let third = tokio::spawn(executor.clone().dispatch_stream(
            "events".to_owned(),
            move |_| {
                third_executed_flag.store(true, AtomicOrdering::Release);
                Ok(())
            },
        ));
        while events_lane.queue_len().unwrap() != 1 {
            tokio::task::yield_now().await;
        }

        release_sender.send(()).unwrap();
        first.await.unwrap().unwrap();
        third.await.unwrap().unwrap();
        assert!(!second_executed.load(AtomicOrdering::Acquire));
        assert!(third_executed.load(AtomicOrdering::Acquire));
    }
}
