//! The settings `run_validation.py load` sends on standard input.
//!
//! The Python side computes the workload from `config/tiers.yaml` and passes
//! the result here as one JSON document. This engine never reads the tier
//! file itself, so the workload model has one implementation only.
//! Credentials come on standard input too, never on the command line, where
//! other users of the machine could read them.

use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct Settings {
    pub connection: Connection,
    pub storage: StorageSettings,
    pub workload: Workload,
    pub out_dir: String,
}

#[derive(Debug, Deserialize)]
pub struct Connection {
    pub endpoint: Option<String>,
    pub region: String,
    pub access_key: String,
    pub secret_key: String,
    pub session_token: Option<String>,
    pub bucket: String,
    /// Every object this run writes lives under this prefix.
    pub prefix: String,
    /// A private certificate authority (CA) bundle, in PEM format.
    pub ca_bundle: Option<String>,
}

/// The `storage.s3` settings of the flavor under test.
#[derive(Debug, Clone, Deserialize)]
pub struct StorageSettings {
    pub force_path_style: bool,
    pub disable_multi_object_delete: bool,
    pub disable_multipart_upload: bool,
    /// `crc32c`, `md5` or `disabled`.
    pub checksum_algorithm: String,
    pub multipart_threshold_bytes: u64,
    pub multipart_part_bytes: u64,
    pub multipart_concurrency: usize,
    pub max_concurrency: usize,
}

#[derive(Debug, Deserialize)]
pub struct Workload {
    pub duration_s: f64,
    pub indexers: usize,
    pub split_bytes: u64,
    pub commit_timeout_s: f64,
    pub merge_factor: usize,
    pub merged_split_cap_bytes: u64,
    pub gc_grace_s: f64,
    pub query_qps: f64,
    pub searchers: usize,
    pub query_profiles: Vec<QueryProfile>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct QueryProfile {
    pub weight: f64,
    pub num_search_fields: usize,
    pub num_terms: usize,
    pub fieldnorm_fields: usize,
    #[serde(default)]
    pub docs_returned: usize,
}

impl Settings {
    pub fn check(&self) -> Result<(), String> {
        let w = &self.workload;
        // Written so that NaN fails too.
        let positive = |x: f64| x > 0.0;
        if !positive(w.duration_s) || !positive(w.commit_timeout_s) || w.query_qps.is_nan() || w.query_qps < 0.0 {
            return Err("duration, commit interval and search rate must be positive".into());
        }
        if w.indexers == 0 || w.searchers == 0 || w.merge_factor < 2 {
            return Err("the workload needs indexers, searchers and a merge factor of 2 or more".into());
        }
        if w.query_profiles.is_empty() || w.query_profiles.iter().any(|p| !positive(p.weight)) {
            return Err("every search profile needs a positive weight".into());
        }
        if !["crc32c", "md5", "disabled"].contains(&self.storage.checksum_algorithm.as_str()) {
            return Err(format!("unknown checksum algorithm {}", self.storage.checksum_algorithm));
        }
        if self.connection.prefix.is_empty() || self.connection.prefix.ends_with('/') {
            return Err("the object prefix must not be empty or end with /".into());
        }
        Ok(())
    }
}
