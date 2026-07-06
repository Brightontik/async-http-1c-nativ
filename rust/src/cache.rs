use crate::https::SSLConfig;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Instant;

#[derive(Debug, Clone, Deserialize)]
pub struct NetworkTask {
    pub task_id: String,
    pub service_name: String,
    pub url: String,
    pub metod: String,
    pub headers: HashMap<String, String>,
    pub payload: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct NetworkResult {
    pub task_id: String,
    pub service_name: String,
    pub status_code: u16,
    pub response_body: String,
    pub error: Option<String>,
    pub unix_timestamp: u64,
}

#[derive(Debug, PartialEq, Eq)]
pub enum PushResult {
    Ok,
    QueueFull,
    RuntimeDisconnected,
}

pub struct AddInCache {
    pub task_sender: crossbeam_channel::Sender<NetworkTask>,
    pub task_receiver: crossbeam_channel::Receiver<NetworkTask>,
    pub output_storage: DashMap<String, NetworkResult>,
    pub max_rows: usize,
    pub max_duration: std::time::Duration,
    pub ssl_config: SSLConfig,
}

impl AddInCache {
    pub fn new(max_rows: usize, secs_timeouts: u64, ssl_config: SSLConfig) -> Self {
        let (sender, receiver) = crossbeam_channel::bounded(max_rows);
        Self {
            task_sender: sender,
            task_receiver: receiver,
            output_storage: DashMap::new(),
            max_rows: max_rows,
            max_duration: std::time::Duration::from_secs(secs_timeouts),
            ssl_config,
        }
    }

    pub fn ack_result(&self, task_ids: Vec<String>) {
        for task_id in task_ids {
            self.output_storage.remove(&task_id);
        }
    }

    pub fn push_task(&self, task: NetworkTask) -> PushResult {
        match self.task_sender.try_send(task) {
            Ok(_) => PushResult::Ok,
            Err(crossbeam_channel::TrySendError::Full(_)) => PushResult::QueueFull,
            Err(crossbeam_channel::TrySendError::Disconnected(_)) => {
                PushResult::RuntimeDisconnected
            }
        }
    }

    pub fn pop_results(&self, filter_service: Option<&str>, limit: usize) -> Vec<NetworkResult> {
        let mut extracted = Vec::new();
        for entry in self.output_storage.iter() {
            if limit > 0 && extracted.len() >= limit {
                break;
            }

            let result = entry.value();

            let mathes_filter = match filter_service {
                Some(service) => result.service_name == service,
                None => true,
            };

            if mathes_filter {
                extracted.push(result.clone());
            }
        }
        extracted
    }

    pub fn clear_expired(&self) {
        use std::time::{SystemTime, UNIX_EPOCH};
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let max_age = self.max_duration.as_secs();

        self.output_storage.retain(|_, result| {
            if now >= result.unix_timestamp && (now - result.unix_timestamp) > max_age {
                false
            } else {
                true
            }
        });
    }
}
