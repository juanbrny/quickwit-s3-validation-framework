//! Result rows, written as JSON Lines in the format the report reads.
//!
//! One row per request: `ts`, `worker`, `op`, `ok`, `latency_s`, `bytes`,
//! `error`. This is the same format the Python engine writes, so the report
//! does not need to know which engine ran.
//!
//! A separate thread writes the files, so a slow disk never delays a request.
//! It flushes at least once a second, so a stopped run keeps its evidence.

use serde::Serialize;
use std::fs::OpenOptions;
use std::io::{BufWriter, Write};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, RecvTimeoutError, Sender};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[derive(Debug, Serialize)]
pub struct Row {
    pub ts: f64,
    pub worker: String,
    pub op: &'static str,
    pub ok: bool,
    pub latency_s: f64,
    pub bytes: u64,
    pub error: Option<String>,
}

#[derive(Clone, Copy)]
pub enum File {
    IngestMerge,
    Query,
}

pub fn now_epoch() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0)
}

#[derive(Clone)]
pub struct Sink {
    tx: Sender<(File, Row)>,
}

impl Sink {
    pub fn write(&self, file: File, row: Row) {
        // The writer only stops after every sender is gone, so this cannot
        // fail while the workload runs.
        let _ = self.tx.send((file, row));
    }
}

pub struct Writer {
    handle: JoinHandle<std::io::Result<u64>>,
    closing: Arc<AtomicBool>,
}

impl Writer {
    /// Wait until every row is on disk. Returns the number of rows written.
    /// A request cut off at the end of the run may still hold a sender, so
    /// the writer also stops once it is told to and the queue is empty.
    pub fn finish(self) -> std::io::Result<u64> {
        self.closing.store(true, Ordering::Release);
        self.handle.join().unwrap_or_else(|_| Err(std::io::Error::other("writer thread panicked")))
    }
}

/// Create both result files. Refuses to overwrite: a run directory is write-once.
pub fn open(dir: &Path) -> std::io::Result<(Sink, Writer)> {
    let create = |name: &str| {
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(dir.join(name))
            .map(|f| BufWriter::with_capacity(1 << 20, f))
    };
    let mut ingest = create("ingest_merge.jsonl")?;
    let mut query = create("query.jsonl")?;
    let (tx, rx) = mpsc::channel::<(File, Row)>();
    let closing = Arc::new(AtomicBool::new(false));
    let told = closing.clone();
    let handle = std::thread::spawn(move || -> std::io::Result<u64> {
        let mut written = 0u64;
        let mut flushed = Instant::now();
        loop {
            match rx.recv_timeout(Duration::from_millis(250)) {
                Ok((file, row)) => {
                    let out = match file {
                        File::IngestMerge => &mut ingest,
                        File::Query => &mut query,
                    };
                    serde_json::to_writer(&mut *out, &row)?;
                    out.write_all(b"\n")?;
                    written += 1;
                }
                Err(RecvTimeoutError::Timeout) if told.load(Ordering::Acquire) => break,
                Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => break,
            }
            if flushed.elapsed() >= Duration::from_secs(1) {
                ingest.flush()?;
                query.flush()?;
                flushed = Instant::now();
            }
        }
        ingest.flush()?;
        query.flush()?;
        Ok(written)
    });
    Ok((Sink { tx }, Writer { handle, closing }))
}
