//! A storage server that stores nothing and answers at once.
//!
//! It measures the load engine, not storage. Run the engine against it, and
//! the engine's own summary shows how many cores and how much memory the
//! workload of a tier needs on this machine. It speaks just enough of the S3
//! protocol for the engine: uploads, multipart uploads, range reads, full
//! reads and bulk deletes. A read returns zeros of the requested length.
//!
//!     cargo run --release --example null_s3 -- 127.0.0.1:9100
//!
//! Plain HTTP only. It is a measuring tool, never a storage system.

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::net::TcpListener;

/// Object sizes only. The bytes themselves are never kept.
type Sizes = Arc<Mutex<HashMap<String, u64>>>;

static ZEROS: [u8; 1 << 20] = [0; 1 << 20];

fn zeros(length: u64) -> Bytes {
    if length as usize <= ZEROS.len() {
        Bytes::from_static(&ZEROS[..length as usize])
    } else {
        Bytes::from(vec![0u8; length as usize])
    }
}

fn reply(status: StatusCode, body: Bytes) -> Response<Full<Bytes>> {
    let mut response = Response::new(Full::new(body));
    *response.status_mut() = status;
    response
}

fn xml(body: String) -> Response<Full<Bytes>> {
    let mut response = reply(StatusCode::OK, Bytes::from(body));
    response.headers_mut().insert("content-type", "application/xml".parse().unwrap());
    response
}

fn range(header: Option<&str>, size: u64) -> Option<(u64, u64)> {
    let spec = header?.strip_prefix("bytes=")?;
    let (start, end) = spec.split_once('-')?;
    let start: u64 = start.parse().ok()?;
    let end: u64 = if end.is_empty() { size.saturating_sub(1) } else { end.parse().ok()? };
    Some((start, end))
}

async fn handle(request: Request<Incoming>, sizes: Sizes) -> Result<Response<Full<Bytes>>, hyper::Error> {
    let method = request.method().clone();
    let path = request.uri().path().to_string();
    let query = request.uri().query().unwrap_or("").to_string();
    let header = |name: &str| request.headers().get(name).and_then(|v| v.to_str().ok()).map(str::to_string);
    let range_header = header("range");
    let is_bucket = path.trim_matches('/').split('/').count() == 1;
    // Read and drop the body, counting its length.
    let mut body = request.into_body();
    let mut length = 0u64;
    while let Some(frame) = body.frame().await {
        if let Ok(data) = frame?.into_data() {
            length += data.len() as u64;
        }
    }
    let response = match method {
        Method::HEAD if is_bucket => reply(StatusCode::OK, Bytes::new()),
        Method::PUT if is_bucket => reply(StatusCode::OK, Bytes::new()),
        Method::POST if query.starts_with("delete") => xml("<DeleteResult></DeleteResult>".into()),
        Method::POST if query.contains("uploads") => xml(
            "<InitiateMultipartUploadResult><UploadId>null</UploadId></InitiateMultipartUploadResult>".into(),
        ),
        Method::POST if query.contains("uploadId") => {
            sizes.lock().unwrap().entry(path.clone()).or_insert(0);
            xml("<CompleteMultipartUploadResult><ETag>\"0\"</ETag></CompleteMultipartUploadResult>".into())
        }
        Method::PUT if query.contains("partNumber") => {
            *sizes.lock().unwrap().entry(path.clone()).or_insert(0) += length;
            let mut r = reply(StatusCode::OK, Bytes::new());
            r.headers_mut().insert("etag", "\"0\"".parse().unwrap());
            r
        }
        Method::PUT => {
            sizes.lock().unwrap().insert(path, length);
            let mut r = reply(StatusCode::OK, Bytes::new());
            r.headers_mut().insert("etag", "\"0\"".parse().unwrap());
            r
        }
        Method::DELETE => {
            sizes.lock().unwrap().remove(&path);
            reply(StatusCode::NO_CONTENT, Bytes::new())
        }
        Method::GET => {
            let size = sizes.lock().unwrap().get(&path).copied().unwrap_or(16 * 1024 * 1024);
            match range(range_header.as_deref(), size) {
                Some((start, end)) => {
                    let end = end.min(size.saturating_sub(1));
                    let mut r = reply(StatusCode::PARTIAL_CONTENT, zeros(end + 1 - start.min(end + 1)));
                    r.headers_mut().insert("content-range", format!("bytes {start}-{end}/{size}").parse().unwrap());
                    r
                }
                None => reply(StatusCode::OK, zeros(size)),
            }
        }
        _ => reply(StatusCode::NOT_IMPLEMENTED, Bytes::new()),
    };
    Ok(response)
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let address = std::env::args().nth(1).unwrap_or_else(|| "127.0.0.1:9100".into());
    let listener = TcpListener::bind(&address).await?;
    eprintln!("null S3 server on http://{address}");
    let sizes: Sizes = Arc::default();
    loop {
        let (stream, _) = listener.accept().await?;
        let sizes = sizes.clone();
        tokio::spawn(async move {
            let service = service_fn(move |request| handle(request, sizes.clone()));
            let _ = http1::Builder::new().serve_connection(TokioIo::new(stream), service).await;
        });
    }
}
