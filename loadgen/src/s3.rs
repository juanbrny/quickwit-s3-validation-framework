//! The S3 operations BYOC sends, with the `storage.s3` settings of a flavor.
//!
//! This is the same library BYOC uses (`aws-sdk-s3`), so the requests on the
//! wire match BYOC's more closely than boto3 can. The rules follow
//! `src/qw_s3_client.py`, which the compatibility checks still use:
//!
//! * a split below the multipart threshold (128 MiB) is one `PutObject`;
//!   at or above it, a multipart upload, unless multipart is disabled;
//! * `checksum_algorithm` decides the only checksum sent on a single upload;
//! * bulk delete is one `DeleteObjects` request, or one `DeleteObject` per
//!   key when the flavor disables it.

use crate::config::{Connection, StorageSettings};
use aws_credential_types::Credentials;
use aws_sdk_s3::config::{
    BehaviorVersion, Region, RequestChecksumCalculation, ResponseChecksumValidation,
};
use aws_sdk_s3::error::{ProvideErrorMetadata, SdkError};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{ChecksumAlgorithm, CompletedMultipartUpload, CompletedPart, Delete, ObjectIdentifier};
use aws_sdk_s3::Client;
use aws_smithy_http_client::tls::{self, TlsContext, TrustStore};
use aws_smithy_types::retry::RetryConfig;
use aws_smithy_types::timeout::TimeoutConfig;
use base64::Engine;
use bytes::Bytes;
use md5::{Digest, Md5};
use std::error::Error;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;

/// What one request did. `runner_fault` marks a limit of the test machine,
/// such as running out of open files. That is never the storage's fault.
#[derive(Debug)]
pub struct Outcome {
    pub op: &'static str,
    pub ok: bool,
    pub latency_s: f64,
    pub bytes: u64,
    pub error: Option<String>,
    pub runner_fault: bool,
}

impl Outcome {
    fn ok(op: &'static str, started: Instant, bytes: u64) -> Self {
        Outcome { op, ok: true, latency_s: started.elapsed().as_secs_f64(), bytes, error: None, runner_fault: false }
    }

    fn failed(op: &'static str, started: Instant, bytes: u64, failure: Failure) -> Self {
        Outcome {
            op,
            ok: false,
            latency_s: started.elapsed().as_secs_f64(),
            bytes,
            error: Some(failure.code),
            runner_fault: failure.runner_fault,
        }
    }
}

pub struct Failure {
    pub code: String,
    pub runner_fault: bool,
}

fn out_of_files(error: &(dyn Error + 'static)) -> bool {
    let mut seen: Option<&(dyn Error + 'static)> = Some(error);
    let mut depth = 0;
    while let Some(e) = seen {
        if let Some(io) = e.downcast_ref::<std::io::Error>()
            && (io.raw_os_error() == Some(libc::EMFILE) || io.raw_os_error() == Some(libc::ENFILE)) {
                return true;
            }
        if e.to_string().contains("Too many open files") {
            return true;
        }
        seen = e.source();
        depth += 1;
        if depth > 16 {
            break;
        }
    }
    false
}

/// Name an error the way the report expects. A service error keeps its S3
/// code, for example `SlowDown` or `NoSuchKey`, because the report counts
/// throttling by that code. Other errors get a fixed name for their kind.
fn classify<E>(error: &SdkError<E, aws_sdk_s3::config::http::HttpResponse>) -> Failure
where
    E: ProvideErrorMetadata + Error + Send + Sync + 'static,
{
    let runner_fault = out_of_files(error);
    let code = match error {
        SdkError::ServiceError(_) => error
            .code()
            .map(str::to_string)
            .or_else(|| error.raw_response().map(|r| r.status().as_u16().to_string()))
            .unwrap_or_else(|| "ServiceError".into()),
        SdkError::TimeoutError(_) => "TimeoutError".into(),
        SdkError::DispatchFailure(d) if d.is_timeout() => "ConnectTimeoutError".into(),
        SdkError::DispatchFailure(_) => "ConnectionError".into(),
        SdkError::ResponseError(_) => error
            .raw_response()
            .map(|r| r.status().as_u16().to_string())
            .unwrap_or_else(|| "ResponseError".into()),
        SdkError::ConstructionFailure(_) => "ConstructionFailure".into(),
        _ => "UnknownError".into(),
    };
    let code = if runner_fault { "TooManyOpenFiles".into() } else { code };
    explain_once(&code, error);
    Failure { code, runner_fault }
}

/// Print the full cause of the first error of each kind. The result files
/// keep only a short name such as `ConnectionError`, which the report counts.
/// That name alone does not tell an operator why a connection failed.
fn explain_once(code: &str, error: &(dyn Error + 'static)) {
    static SEEN: std::sync::Mutex<Vec<String>> = std::sync::Mutex::new(Vec::new());
    let mut seen = SEEN.lock().unwrap();
    if seen.iter().any(|c| c == code) {
        return;
    }
    seen.push(code.to_string());
    eprintln!("load engine: first {code}: {}", aws_smithy_types::error::display::DisplayErrorContext(error));
}

fn stream_failure(error: &(dyn Error + 'static)) -> Failure {
    let runner_fault = out_of_files(error);
    let code = if runner_fault { "TooManyOpenFiles" } else { "ResponseStreamError" };
    explain_once(code, error);
    Failure { code: code.into(), runner_fault }
}

pub struct Store {
    client: Client,
    bucket: String,
    settings: StorageSettings,
    permits: Arc<Semaphore>,
}

pub fn build_client(connection: &Connection, settings: &StorageSettings) -> Result<Client, String> {
    let trust = match &connection.ca_bundle {
        // Like boto3's `verify=<path>`: the bundle replaces the system roots.
        Some(path) => {
            let pem = std::fs::read(path).map_err(|e| format!("cannot read the CA bundle {path}: {e}"))?;
            TrustStore::empty().with_native_roots(false).with_pem_certificate(pem)
        }
        None => TrustStore::empty().with_native_roots(true),
    };
    let tls_context = TlsContext::builder()
        .with_trust_store(trust)
        .build()
        .map_err(|e| format!("cannot set up TLS: {e}"))?;
    let http = aws_smithy_http_client::Builder::new()
        .tls_provider(tls::Provider::Rustls(tls::rustls_provider::CryptoMode::Ring))
        .tls_context(tls_context)
        .build_https();
    let credentials = Credentials::new(
        &connection.access_key,
        &connection.secret_key,
        connection.session_token.clone(),
        None,
        "byoc-validation",
    );
    let mut config = aws_sdk_s3::Config::builder()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new(connection.region.clone()))
        .credentials_provider(credentials)
        .http_client(http)
        .force_path_style(settings.force_path_style)
        // The SDK adds a CRC32 checksum to every upload by default. With
        // `WhenRequired`, only the checksum the flavor selects is sent. This
        // is the same rule `boto_config()` applies on the Python side.
        .request_checksum_calculation(RequestChecksumCalculation::WhenRequired)
        .response_checksum_validation(ResponseChecksumValidation::WhenRequired)
        // Three attempts in standard mode, the same as the Python engine.
        .retry_config(RetryConfig::standard().with_max_attempts(3))
        .timeout_config(
            TimeoutConfig::builder()
                .connect_timeout(Duration::from_secs(10))
                .read_timeout(Duration::from_secs(60))
                .build(),
        );
    if let Some(endpoint) = &connection.endpoint {
        config = config.endpoint_url(endpoint);
    }
    Ok(Client::from_conf(config.build()))
}

pub fn md5_base64(data: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(Md5::digest(data))
}

impl Store {
    pub fn new(client: Client, bucket: String, settings: StorageSettings) -> Self {
        let permits = Arc::new(Semaphore::new(settings.max_concurrency.max(1)));
        Store { client, bucket, settings, permits }
    }

    /// Upload one split. `md5` is precomputed off the async threads when the
    /// flavor needs it, because hashing 200 MB takes a while.
    pub async fn put_split(&self, key: &str, data: Bytes, md5: Option<String>) -> Outcome {
        let size = data.len() as u64;
        if self.settings.disable_multipart_upload || size < self.settings.multipart_threshold_bytes {
            let _permit = self.permits.acquire().await;
            let started = Instant::now();
            let mut request = self.client.put_object().bucket(&self.bucket).key(key).body(ByteStream::from(data));
            match self.settings.checksum_algorithm.as_str() {
                "crc32c" => request = request.checksum_algorithm(ChecksumAlgorithm::Crc32C),
                "md5" => request = request.set_content_md5(md5),
                _ => {}
            }
            return match request.send().await {
                Ok(_) => Outcome::ok("put_object", started, size),
                Err(e) => Outcome::failed("put_object", started, size, classify(&e)),
            };
        }
        self.multipart(key, data).await
    }

    async fn multipart(&self, key: &str, data: Bytes) -> Outcome {
        let size = data.len() as u64;
        let started = Instant::now();
        let created = {
            let _permit = self.permits.acquire().await;
            self.client.create_multipart_upload().bucket(&self.bucket).key(key).send().await
        };
        let upload_id = match created {
            Ok(r) => r.upload_id.unwrap_or_default(),
            Err(e) => return Outcome::failed("multipart_upload", started, size, classify(&e)),
        };
        let part = self.settings.multipart_part_bytes.max(5 * 1024 * 1024) as usize;
        let ranges: Vec<(i32, usize, usize)> = (0..data.len())
            .step_by(part)
            .enumerate()
            .map(|(i, offset)| (i as i32 + 1, offset, (offset + part).min(data.len())))
            .collect();
        // BYOC uploads parts concurrently. A storage system that serializes
        // concurrent UploadPart calls would slow every large upload.
        let limit = Arc::new(Semaphore::new(self.settings.multipart_concurrency.max(1)));
        let uploads = ranges.iter().map(|&(number, start, end)| {
            let limit = limit.clone();
            let body = data.slice(start..end);
            let upload_id = upload_id.clone();
            async move {
                let _slot = limit.acquire().await;
                let _permit = self.permits.acquire().await;
                self.client
                    .upload_part()
                    .bucket(&self.bucket)
                    .key(key)
                    .upload_id(upload_id)
                    .part_number(number)
                    .body(ByteStream::from(body))
                    .send()
                    .await
                    .map(|r| CompletedPart::builder().part_number(number).set_e_tag(r.e_tag).build())
            }
        });
        let mut parts = Vec::with_capacity(ranges.len());
        for result in futures::future::join_all(uploads).await {
            match result {
                Ok(p) => parts.push(p),
                Err(e) => {
                    self.abort(key, &upload_id).await;
                    return Outcome::failed("multipart_upload", started, size, classify(&e));
                }
            }
        }
        let completed = {
            let _permit = self.permits.acquire().await;
            self.client
                .complete_multipart_upload()
                .bucket(&self.bucket)
                .key(key)
                .upload_id(&upload_id)
                .multipart_upload(CompletedMultipartUpload::builder().set_parts(Some(parts)).build())
                .send()
                .await
        };
        match completed {
            Ok(_) => Outcome::ok("multipart_upload", started, size),
            Err(e) => {
                self.abort(key, &upload_id).await;
                Outcome::failed("multipart_upload", started, size, classify(&e))
            }
        }
    }

    async fn abort(&self, key: &str, upload_id: &str) {
        let _ = self.client.abort_multipart_upload().bucket(&self.bucket).key(key).upload_id(upload_id).send().await;
    }

    /// Read a whole split, as a merge does. The body is counted and dropped.
    pub async fn get_full(&self, key: &str) -> Outcome {
        self.get("get_object_full", key, None).await
    }

    /// Read a byte range, as a search does. `end` is inclusive.
    pub async fn get_range(&self, op: &'static str, key: &str, start: u64, end: u64) -> Outcome {
        self.get(op, key, Some(format!("bytes={start}-{end}"))).await
    }

    async fn get(&self, op: &'static str, key: &str, range: Option<String>) -> Outcome {
        let _permit = self.permits.acquire().await;
        let started = Instant::now();
        let response = match self.client.get_object().bucket(&self.bucket).key(key).set_range(range).send().await {
            Ok(r) => r,
            Err(e) => return Outcome::failed(op, started, 0, classify(&e)),
        };
        let mut body = response.body;
        let mut bytes = 0u64;
        loop {
            match body.try_next().await {
                Ok(Some(chunk)) => bytes += chunk.len() as u64,
                Ok(None) => return Outcome::ok(op, started, bytes),
                Err(e) => return Outcome::failed(op, started, bytes, stream_failure(&e)),
            }
        }
    }

    /// Delete the inputs of a merge, as the garbage collector does.
    pub async fn delete_batch(&self, keys: &[String]) -> Outcome {
        let _permit = self.permits.acquire().await;
        let started = Instant::now();
        if self.settings.disable_multi_object_delete {
            let mut failure = None;
            for key in keys {
                if let Err(e) = self.client.delete_object().bucket(&self.bucket).key(key).send().await {
                    failure.get_or_insert(classify(&e));
                }
            }
            return match failure {
                None => Outcome::ok("delete_object_loop", started, 0),
                Some(f) => Outcome::failed("delete_object_loop", started, 0, f),
            };
        }
        let objects: Vec<ObjectIdentifier> = keys
            .iter()
            .filter_map(|k| ObjectIdentifier::builder().key(k).build().ok())
            .collect();
        let delete = match Delete::builder().set_objects(Some(objects)).quiet(true).build() {
            Ok(d) => d,
            Err(_) => return Outcome::failed("delete_objects_bulk", started, 0, Failure { code: "ConstructionFailure".into(), runner_fault: false }),
        };
        match self.client.delete_objects().bucket(&self.bucket).delete(delete).send().await {
            Ok(r) if r.errors().is_empty() => Outcome::ok("delete_objects_bulk", started, 0),
            Ok(r) => {
                let code = r.errors()[0].code().unwrap_or("DeleteError").to_string();
                Outcome::failed("delete_objects_bulk", started, 0, Failure { code, runner_fault: false })
            }
            Err(e) => Outcome::failed("delete_objects_bulk", started, 0, classify(&e)),
        }
    }
}
