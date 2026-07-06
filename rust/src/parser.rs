use serde::Deserialize;
use std::collections::HashMap;

use crate::{
    cache::{NetworkResult, NetworkTask},
    utils::safe_deserialize_json,
};

fn default_method() -> String {
    "GET".to_string()
}

fn default_max_rows() -> usize {
    1000
}
fn default_timeout() -> u64 {
    30
}

#[derive(Debug, Clone, Deserialize)]
pub struct AddInTaskManifest {
    pub task_id: String,
    pub service_name: String,
    pub url: String,
    #[serde(default = "default_method")]
    pub method: String,
    #[serde(default)]
    pub headers: HashMap<String, String>,
    #[serde(default)]
    pub payload: String,
}

#[derive(Debug, Deserialize)]
pub struct AddInBatchManifest {
    pub tasks: Vec<AddInTaskManifest>,
}

#[derive(Debug, Deserialize)]
pub struct AddInSettingsManifest {
    #[serde(default = "default_max_rows")]
    pub max_rows: usize,

    #[serde(default = "default_timeout")]
    pub secs_timeouts: u64,

    #[serde(default)]
    pub danger_accept_invalid_certs: bool,

    pub ca_cert_path: Option<String>,

    pub client_cert_path: Option<String>,

    pub client_key_path: Option<String>,
}

pub fn process_incoming_json(json_from_1c: &str) -> Result<Vec<NetworkTask>, String> {
    let batch: AddInBatchManifest = safe_deserialize_json(json_from_1c)?;

    let network_tasks = batch
        .tasks
        .into_iter()
        .map(|t| NetworkTask {
            task_id: t.task_id,
            service_name: t.service_name,
            url: t.url,
            metod: t.method,
            headers: t.headers,
            payload: t.payload,
        })
        .collect();

    Ok(network_tasks)
}
