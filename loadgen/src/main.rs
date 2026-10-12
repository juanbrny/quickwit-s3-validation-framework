//! BYOC load engine.
//!
//! `run_validation.py load` starts this program, sends its settings as JSON
//! on standard input, and reads two JSON lines from standard output: one
//! when the workload starts, and one summary when it ends. The results go
//! to `ingest_merge.jsonl` and `query.jsonl` in the run directory.
//!
//! It exists because one Python process could not send the workload of the
//! larger tiers. Python runs its code on one core at a time, and in one AWS
//! run the test machine, not S3, set the response times. This engine uses
//! every core, and the same S3 library as BYOC.

mod config;
mod results;
mod s3;
mod workload;

use bytes::Bytes;
use serde_json::json;
use std::io::Read;
use std::path::Path;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;

static PANICS: Mutex<Vec<String>> = Mutex::new(Vec::new());
static STOP: OnceLock<CancellationToken> = OnceLock::new();

/// Raise the open-file limit to the hard limit. Every request in flight holds
/// a socket, and a socket needs a file descriptor.
fn raise_file_limit() -> u64 {
    unsafe {
        let mut limit = libc::rlimit { rlim_cur: 0, rlim_max: 0 };
        if libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) != 0 {
            return 0;
        }
        // macOS refuses values above OPEN_MAX even when the hard limit is
        // "unlimited", so try the hard limit first, then a safe ceiling.
        for target in [limit.rlim_max.min(1 << 20), 65_536, 24_576, 10_240] {
            if target <= limit.rlim_cur || target > limit.rlim_max {
                continue;
            }
            let wanted = libc::rlimit { rlim_cur: target, rlim_max: limit.rlim_max };
            if libc::setrlimit(libc::RLIMIT_NOFILE, &wanted) == 0 {
                return target;
            }
        }
        limit.rlim_cur
    }
}

fn cpu_seconds() -> (f64, f64, u64) {
    unsafe {
        let mut usage: libc::rusage = std::mem::zeroed();
        libc::getrusage(libc::RUSAGE_SELF, &mut usage);
        let seconds = |t: libc::timeval| t.tv_sec as f64 + t.tv_usec as f64 / 1e6;
        // Linux reports the peak memory in KiB, macOS in bytes.
        let peak = if cfg!(target_os = "macos") { usage.ru_maxrss as u64 } else { usage.ru_maxrss as u64 * 1024 };
        (seconds(usage.ru_utime), seconds(usage.ru_stime), peak)
    }
}

fn percentile(sorted: &[f32], pct: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let index = ((sorted.len() - 1) as f64 * pct / 100.0).round() as usize;
    sorted[index.min(sorted.len() - 1)] as f64
}

fn fail(message: &str) -> ! {
    println!("{}", json!({"event": "error", "message": message}));
    std::process::exit(2);
}

fn main() {
    let mut input = String::new();
    if std::io::stdin().read_to_string(&mut input).is_err() {
        fail("cannot read the settings from standard input");
    }
    let settings: config::Settings = serde_json::from_str(&input).unwrap_or_else(|e| fail(&format!("invalid settings: {e}")));
    if let Err(e) = settings.check() {
        fail(&e);
    }
    let file_limit = raise_file_limit();
    // A quarter of the cores for uploads and merges, the rest for searches.
    let cores = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(4);
    let pool = |name: &str, threads: usize| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(threads.max(2))
            .thread_name(name)
            .enable_all()
            .build()
            .unwrap_or_else(|e| fail(&format!("cannot start the {name} threads: {e}")))
    };
    let ingest = pool("ingest", cores / 4);
    let search = pool("search", cores - cores / 4);
    let summary = search.block_on(run(settings, file_limit, ingest.handle().clone()));
    println!("{summary}");
    // Do not wait for requests that were cut off at the end of the run.
    ingest.shutdown_timeout(Duration::from_secs(1));
    search.shutdown_timeout(Duration::from_secs(1));
}

async fn run(settings: config::Settings, file_limit: u64, ingest: tokio::runtime::Handle) -> serde_json::Value {
    let config::Settings { connection, storage, workload, out_dir } = settings;
    // One client per thread pool, so each keeps its connections on its own threads.
    let client = || s3::build_client(&connection, &storage).unwrap_or_else(|e| fail(&e));
    let md5_needed = storage.checksum_algorithm == "md5";
    let search_store = s3::Store::new(client(), connection.bucket.clone(), storage.clone());
    let store = s3::Store::new(client(), connection.bucket.clone(), storage);
    let (sink, writer) = results::open(Path::new(&out_dir)).unwrap_or_else(|e| fail(&format!("cannot create the result files: {e}")));

    // One random buffer for every upload. Each upload takes a slice of it at
    // a random offset, so no two uploads hold the same bytes, and memory
    // stays near this buffer's size whatever the number of indexers.
    let largest = workload.split_bytes.max(workload.merged_split_cap_bytes) as usize;
    let mut buffer = vec![0u8; largest + 64 * 1024 * 1024];
    rand::fill(&mut buffer[..]);
    let engine = Arc::new(workload::Engine::new(
        (store, ingest),
        (search_store, tokio::runtime::Handle::current()),
        sink,
        Bytes::from(buffer),
        md5_needed,
    ));
    let _ = STOP.set(engine.stop.clone());
    std::panic::set_hook(Box::new(|info| {
        PANICS.lock().unwrap().push(info.to_string());
        if let Some(stop) = STOP.get() {
            stop.cancel();
        }
    }));

    let workload = Arc::new(workload);
    let started_epoch = results::now_epoch();
    let started = Instant::now();
    println!("{}", json!({"event": "started", "epoch": started_epoch, "file_limit": file_limit}));
    workload::start(&engine, &workload, &connection.prefix);

    let duration = Duration::from_secs_f64(workload.duration_s);
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).ok();
    let stopped_by = tokio::select! {
        _ = tokio::time::sleep(duration) => "duration",
        _ = engine.stop.cancelled() => "error",
        _ = tokio::signal::ctrl_c() => "interrupted",
        _ = async { match terminate.as_mut() { Some(s) => { s.recv().await; } None => std::future::pending::<()>().await } } => "interrupted",
    };
    let measured = started.elapsed().as_secs_f64();
    engine.stop.cancel();
    engine.tasks.close();
    let _ = tokio::time::timeout(Duration::from_secs(30), engine.tasks.wait()).await;

    let stats = &engine.stats;
    let mut lags = std::mem::take(&mut *stats.lags.lock().unwrap());
    lags.sort_by(|a, b| a.total_cmp(b));
    let late = lags.iter().filter(|&&l| l > 0.1).count();
    let (user, system, peak_memory) = cpu_seconds();
    let cores = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1);
    let mut worker_errors: Vec<serde_json::Value> = Vec::new();
    if let Some(message) = engine.fatal.lock().unwrap().clone() {
        worker_errors.push(json!({"worker": "engine", "type": "RunnerLimitError", "message": message}));
    }
    for message in PANICS.lock().unwrap().iter() {
        worker_errors.push(json!({"worker": "engine", "type": "panic", "message": message}));
    }
    let live_splits = engine.registry.len();
    let queries = json!({
        "target_qps": workload.query_qps,
        "scheduled": stats.scheduled.load(Ordering::Relaxed),
        "started": stats.started.load(Ordering::Relaxed),
        "skipped_no_splits": stats.skipped_no_splits.load(Ordering::Relaxed),
        "skipped_backlog": stats.skipped_backlog.load(Ordering::Relaxed),
        "peak_in_flight": stats.peak_in_flight.load(Ordering::Relaxed),
        "late_over_100ms": late,
        "start_delay_p50_s": percentile(&lags, 50.0),
        "start_delay_p99_s": percentile(&lags, 99.0),
        "start_delay_max_s": lags.last().copied().unwrap_or(0.0) as f64,
    });
    drop(engine);
    let rows = writer.finish().unwrap_or_else(|e| fail(&format!("cannot write the result files: {e}")));
    json!({
        "event": "finished",
        "engine": "rust",
        "engine_version": env!("CARGO_PKG_VERSION"),
        "started_epoch": started_epoch,
        "measurement_duration_s": measured.min(workload.duration_s),
        "stopped_by": stopped_by,
        "rows_written": rows,
        "live_splits_at_end": live_splits,
        "searches": queries,
        "machine": {
            "cores": cores,
            "cpu_user_s": user,
            "cpu_system_s": system,
            "average_cores_used": (user + system) / measured.max(1e-9),
            "peak_memory_bytes": peak_memory,
            "file_limit": file_limit,
        },
        "worker_errors": worker_errors,
    })
}
