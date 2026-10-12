//! The BYOC storage workload: indexers, merges, garbage collection and search.
//!
//! It follows `docs/background/01_s3_interaction_analysis.md`. Three rules
//! differ from the first Python engine, because that engine got them wrong:
//!
//! 1. Indexers start spread over one commit interval. Started together, 135
//!    indexers at the 100TB tier uploaded 26 GB at the same second every
//!    minute, and the test then measured the test machine's network card.
//! 2. A merge runs in its own task. It never blocks the next upload, the same
//!    as BYOC, where indexing and merging are separate pipelines.
//! 3. A merge unpublishes its inputs first, and the garbage collector deletes
//!    them only after a grace period. Deleting first made searches read files
//!    that were already gone: 440 `NoSuchKey` errors in one AWS run.
//!
//! Uploads and searches run on separate thread pools, each with its own
//! S3 client. Before signing an upload, the S3 library hashes its whole body.
//! For a 196 MB split, that holds a thread for about 120 ms. On a shared
//! pool, that delayed the search timers, and 2% of searches started late.
//!
//! Searches are open loop: they start on a fixed schedule, whether or not
//! earlier searches have finished, the way user queries arrive. If the
//! engine itself falls behind that schedule, it counts the delay, so the
//! report can tell a slow test machine from slow storage.

use crate::config::{QueryProfile, Workload};
use crate::results::{now_epoch, File, Row, Sink};
use crate::s3::{md5_base64, Outcome, Store};
use bytes::Bytes;
use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

pub const FOOTER_RANGE_BYTES: u64 = 64 * 1024;
pub const TERM_LOOKUP_RANGE_BYTES: u64 = 8 * 1024;
pub const DOC_FETCH_RANGE_BYTES: u64 = 2 * 1024;
/// Range reads start anywhere in the first 10 MiB of a split, as in the Python engine.
const RANGE_START_MAX: u64 = 10 * 1024 * 1024;

/// Published splits: the ones a search may read.
#[derive(Default)]
pub struct Registry {
    keys: RwLock<Vec<String>>,
}

impl Registry {
    pub fn publish(&self, key: String) {
        self.keys.write().unwrap().push(key);
    }

    /// Replace merged inputs with their merged output, in one step, so a
    /// search never sees both or neither.
    pub fn replace(&self, inputs: &[String], merged: Option<String>) {
        let gone: HashSet<&String> = inputs.iter().collect();
        let mut keys = self.keys.write().unwrap();
        keys.retain(|k| !gone.contains(k));
        if let Some(m) = merged {
            keys.push(m);
        }
    }

    pub fn len(&self) -> usize {
        self.keys.read().unwrap().len()
    }

    /// Up to `count` different published splits, chosen at random.
    pub fn sample(&self, count: usize) -> Vec<String> {
        let keys = self.keys.read().unwrap();
        let count = count.min(keys.len());
        let mut rng = rand::rng();
        rand::seq::index::sample(&mut rng, keys.len(), count)
            .into_iter()
            .map(|i| keys[i].clone())
            .collect()
    }
}

/// How well the engine kept to its own schedule.
#[derive(Default)]
pub struct SearchStats {
    pub scheduled: AtomicU64,
    pub started: AtomicU64,
    pub skipped_no_splits: AtomicU64,
    pub skipped_backlog: AtomicU64,
    pub in_flight: AtomicUsize,
    pub peak_in_flight: AtomicUsize,
    /// Start delay of every search, in seconds.
    pub lags: Mutex<Vec<f32>>,
}

pub struct Engine {
    /// Uploads, merges and deletes.
    pub store: Store,
    /// Searches only.
    pub search_store: Store,
    pub ingest_pool: tokio::runtime::Handle,
    pub search_pool: tokio::runtime::Handle,
    pub sink: Sink,
    pub registry: Registry,
    pub stats: SearchStats,
    pub stop: CancellationToken,
    pub tasks: TaskTracker,
    pub fatal: Mutex<Option<String>>,
    pub payload: Bytes,
    pub md5_needed: bool,
    upload_seq: AtomicU64,
}

impl Engine {
    pub fn new(
        (store, ingest_pool): (Store, tokio::runtime::Handle),
        (search_store, search_pool): (Store, tokio::runtime::Handle),
        sink: Sink,
        payload: Bytes,
        md5_needed: bool,
    ) -> Self {
        Engine {
            store,
            search_store,
            ingest_pool,
            search_pool,
            sink,
            registry: Registry::default(),
            stats: SearchStats::default(),
            stop: CancellationToken::new(),
            tasks: TaskTracker::new(),
            fatal: Mutex::new(None),
            payload,
            md5_needed,
            upload_seq: AtomicU64::new(0),
        }
    }

    /// Stop the run because the test machine hit a limit.
    fn runner_fault(&self, op: &str) {
        let mut fatal = self.fatal.lock().unwrap();
        if fatal.is_none() {
            *fatal = Some(format!(
                "This machine ran out of open files during {op}. That is a limit of the test \
                 machine, not of the storage. Run `ulimit -n 65536` in this shell and start again."
            ));
        }
        self.stop.cancel();
    }

    fn record(&self, file: File, worker: &str, outcome: &Outcome) {
        if outcome.runner_fault {
            self.runner_fault(outcome.op);
            return;
        }
        self.sink.write(
            file,
            Row {
                ts: now_epoch(),
                worker: worker.to_string(),
                op: outcome.op,
                ok: outcome.ok,
                latency_s: outcome.latency_s,
                bytes: outcome.bytes,
                error: outcome.error.clone(),
            },
        );
    }

    /// A slice of the shared random buffer, at a random offset. Every upload
    /// gets different bytes without allocating a new buffer each time.
    async fn payload(&self, size: u64) -> (Bytes, Option<String>) {
        let size = (size as usize).clamp(1024, self.payload.len());
        let start = rand::random_range(0..=self.payload.len() - size);
        let data = self.payload.slice(start..start + size);
        let md5 = if self.md5_needed {
            let copy = data.clone();
            tokio::task::spawn_blocking(move || md5_base64(&copy)).await.ok()
        } else {
            None
        };
        (data, md5)
    }

    fn next_seq(&self) -> u64 {
        self.upload_seq.fetch_add(1, Ordering::Relaxed)
    }
}

pub fn start(engine: &Arc<Engine>, workload: &Arc<Workload>, prefix: &str) {
    let indexers = workload.indexers;
    for node in 0..indexers {
        let offset = workload.commit_timeout_s * node as f64 / indexers as f64;
        let (e, w, p) = (engine.clone(), workload.clone(), prefix.to_string());
        engine.tasks.spawn_on(async move {
            let stop = e.stop.clone();
            tokio::select! {
                _ = stop.cancelled() => {}
                _ = indexer(e, w, p, node, offset) => {}
            }
        }, &engine.ingest_pool);
    }
    if workload.query_qps > 0.0 {
        let (e, w) = (engine.clone(), workload.clone());
        engine.tasks.spawn_on(async move {
            let stop = e.stop.clone();
            tokio::select! {
                _ = stop.cancelled() => {}
                _ = search_schedule(e, w) => {}
            }
        }, &engine.search_pool);
    }
}

async fn indexer(engine: Arc<Engine>, workload: Arc<Workload>, prefix: String, node: usize, offset_s: f64) {
    let worker = format!("indexer-{node}");
    let interval = Duration::from_secs_f64(workload.commit_timeout_s);
    let mut next = Instant::now() + Duration::from_secs_f64(offset_s);
    let mut produced: Vec<String> = Vec::new();
    let mut seq = 0u64;
    loop {
        tokio::time::sleep_until(next).await;
        next += interval;
        seq += 1;
        let key = format!("{prefix}/node{node}/immature-{seq}-{}.split", engine.next_seq());
        let (data, md5) = engine.payload(workload.split_bytes).await;
        let outcome = engine.store.put_split(&key, data, md5).await;
        engine.record(File::IngestMerge, &worker, &outcome);
        if outcome.ok {
            engine.registry.publish(key.clone());
            produced.push(key);
        }
        if produced.len() >= workload.merge_factor {
            let inputs: Vec<String> = produced.drain(..workload.merge_factor).collect();
            let (e, w, p) = (engine.clone(), workload.clone(), prefix.clone());
            engine.tasks.spawn_on(async move {
                let stop = e.stop.clone();
                tokio::select! {
                    _ = stop.cancelled() => {}
                    _ = merge(e, w, p, node, inputs) => {}
                }
            }, &engine.ingest_pool);
        }
        // An upload slower than the commit interval delays the next one, as
        // in BYOC. The schedule does not try to catch up with a burst.
        let now = Instant::now();
        if next < now {
            next = now;
        }
    }
}

async fn merge(engine: Arc<Engine>, workload: Arc<Workload>, prefix: String, node: usize, inputs: Vec<String>) {
    let worker = format!("merger-{node}");
    let mut total = 0u64;
    for key in &inputs {
        let outcome = engine.store.get_full(key).await;
        engine.record(File::IngestMerge, &worker, &outcome);
        total += outcome.bytes;
    }
    // The merged split is capped for test cost, above the multipart
    // threshold so the multipart path runs. Full-size merges are tested at a
    // later stage.
    let key = format!("{prefix}/node{node}/merged-{}.split", engine.next_seq());
    let (data, md5) = engine.payload(total.min(workload.merged_split_cap_bytes)).await;
    let outcome = engine.store.put_split(&key, data, md5).await;
    engine.record(File::IngestMerge, &worker, &outcome);
    // Publish: the merged split replaces its inputs for every new search.
    engine.registry.replace(&inputs, outcome.ok.then_some(key));
    // Searches that started before the publish may still read the inputs.
    tokio::time::sleep(Duration::from_secs_f64(workload.gc_grace_s)).await;
    let outcome = engine.store.delete_batch(&inputs).await;
    engine.record(File::IngestMerge, &format!("gc-{node}"), &outcome);
}

fn pick_profile(profiles: &[QueryProfile]) -> &QueryProfile {
    let total: f64 = profiles.iter().map(|p| p.weight).sum();
    let mut point = rand::random_range(0.0..total);
    for p in profiles {
        if point < p.weight {
            return p;
        }
        point -= p.weight;
    }
    &profiles[profiles.len() - 1]
}

/// The range reads one search needs, from the documented formula:
/// `GETs = splits x (fields x terms x 3 + fieldnorm + 1) + docs_returned`,
/// plus one footer read the first time a searcher touches a split.
pub fn query_jobs(keys: &[String], profile: &QueryProfile, warm: &Mutex<HashSet<String>>) -> Vec<(&'static str, String, u64, u64)> {
    let mut jobs = Vec::new();
    let per_split = profile.num_search_fields * profile.num_terms * 3 + profile.fieldnorm_fields + 1;
    {
        let mut warm = warm.lock().unwrap();
        for key in keys {
            if warm.insert(key.clone()) {
                jobs.push(("get_footer", key.clone(), 0, FOOTER_RANGE_BYTES - 1));
            }
            for _ in 0..per_split {
                let start = rand::random_range(0..=RANGE_START_MAX);
                jobs.push(("get_term_or_field", key.clone(), start, start + TERM_LOOKUP_RANGE_BYTES - 1));
            }
        }
    }
    // Documents are added once per search, spread over the splits it hit.
    for i in 0..profile.docs_returned {
        let start = rand::random_range(0..=RANGE_START_MAX);
        jobs.push(("get_doc", keys[i % keys.len()].clone(), start, start + DOC_FETCH_RANGE_BYTES - 1));
    }
    jobs
}

async fn search_schedule(engine: Arc<Engine>, workload: Arc<Workload>) {
    let caches: Arc<Vec<Mutex<HashSet<String>>>> =
        Arc::new((0..workload.searchers).map(|_| Mutex::new(HashSet::new())).collect());
    let started = Instant::now();
    // A backlog of 30 seconds of searches means the storage cannot keep up.
    // Beyond that, new searches are counted as skipped instead of piling up.
    let backlog_cap = ((workload.query_qps * 30.0) as usize).max(100);
    let mut n: u64 = 0;
    loop {
        let due = started + Duration::from_secs_f64(n as f64 / workload.query_qps);
        tokio::time::sleep_until(due).await;
        let lag = Instant::now().saturating_duration_since(due).as_secs_f32();
        let searcher = (n as usize) % workload.searchers;
        n += 1;
        let stats = &engine.stats;
        stats.scheduled.fetch_add(1, Ordering::Relaxed);
        if engine.registry.len() == 0 {
            stats.skipped_no_splits.fetch_add(1, Ordering::Relaxed);
            continue;
        }
        if stats.in_flight.load(Ordering::Relaxed) >= backlog_cap {
            stats.skipped_backlog.fetch_add(1, Ordering::Relaxed);
            continue;
        }
        stats.lags.lock().unwrap().push(lag);
        stats.started.fetch_add(1, Ordering::Relaxed);
        let now = stats.in_flight.fetch_add(1, Ordering::Relaxed) + 1;
        stats.peak_in_flight.fetch_max(now, Ordering::Relaxed);
        let (e, w, c) = (engine.clone(), workload.clone(), caches.clone());
        engine.tasks.spawn_on(async move {
            let stop = e.stop.clone();
            tokio::select! {
                _ = stop.cancelled() => {}
                _ = search(e.clone(), w, c, searcher) => {}
            }
            e.stats.in_flight.fetch_sub(1, Ordering::Relaxed);
        }, &engine.search_pool);
    }
}

async fn search(engine: Arc<Engine>, workload: Arc<Workload>, caches: Arc<Vec<Mutex<HashSet<String>>>>, searcher: usize) {
    let profile = pick_profile(&workload.query_profiles);
    let count = rand::random_range(1..=5usize);
    let keys = engine.registry.sample(count);
    if keys.is_empty() {
        return;
    }
    let jobs = query_jobs(&keys, profile, &caches[searcher]);
    let started = Instant::now();
    // Every read of one search goes out at once. This is how BYOC hides the
    // latency of each single request.
    // Each read is recorded when it ends, so its time stamp is accurate.
    let reads = jobs.iter().map(|(op, key, start, end)| {
        let engine = &engine;
        async move {
            let outcome = engine.search_store.get_range(op, key, *start, *end).await;
            engine.record(File::Query, "searcher", &outcome);
            outcome.ok
        }
    });
    let all_ok = futures::future::join_all(reads).await.into_iter().all(|ok| ok);
    let wall_clock = started.elapsed().as_secs_f64();
    engine.sink.write(
        File::Query,
        Row {
            ts: now_epoch(),
            worker: "searcher".into(),
            op: "query_wall_clock",
            ok: all_ok,
            latency_s: wall_clock,
            bytes: 0,
            error: if all_ok { None } else { Some("one_or_more_gets_failed".into()) },
        },
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    fn profile(fields: usize, terms: usize, fieldnorm: usize, docs: usize) -> QueryProfile {
        QueryProfile { weight: 1.0, num_search_fields: fields, num_terms: terms, fieldnorm_fields: fieldnorm, docs_returned: docs }
    }

    #[test]
    fn a_search_sends_the_documented_number_of_reads() {
        // GETs = splits x (fields x terms x 3 + fieldnorm + 1) + docs_returned,
        // plus one footer per split the searcher has not seen yet.
        let keys: Vec<String> = (0..3).map(|i| format!("s{i}")).collect();
        let warm = Mutex::new(HashSet::new());
        let p = profile(3, 2, 2, 50);
        let cold = query_jobs(&keys, &p, &warm);
        assert_eq!(cold.len(), 3 * (3 * 2 * 3 + 2 + 1) + 50 + 3);
        let warm_again = query_jobs(&keys, &p, &warm);
        assert_eq!(warm_again.len(), 3 * 21 + 50, "footers are read once per split");
        assert_eq!(warm_again.iter().filter(|j| j.0 == "get_doc").count(), 50, "documents once per search, not per split");
    }

    #[test]
    fn reads_stay_inside_their_modeled_sizes() {
        let warm = Mutex::new(HashSet::new());
        for (op, _, start, end) in query_jobs(&["s".to_string()], &profile(1, 1, 1, 20), &warm) {
            let size = end - start + 1;
            match op {
                "get_footer" => assert_eq!(size, FOOTER_RANGE_BYTES),
                "get_term_or_field" => assert_eq!(size, TERM_LOOKUP_RANGE_BYTES),
                "get_doc" => assert_eq!(size, DOC_FETCH_RANGE_BYTES),
                other => panic!("unexpected read {other}"),
            }
        }
    }

    #[test]
    fn a_merge_replaces_its_inputs_in_one_step() {
        let registry = Registry::default();
        for key in ["a", "b", "c"] {
            registry.publish(key.into());
        }
        registry.replace(&["a".into(), "b".into()], Some("m".into()));
        let mut seen = registry.sample(10);
        seen.sort();
        assert_eq!(seen, vec!["c".to_string(), "m".to_string()]);
    }
}
