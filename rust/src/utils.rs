use flate2::write::GzEncoder;
use flate2::Compression;
use reqwest::Client;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::{
    future::Future,
    time::{Duration, Instant},
};
use tokio_retry2::{
    strategy::{jitter, ExponentialFactorBackoff},
    Retry,
};

#[derive(serde::Serialize)]
pub struct PingResult {
    pub is_alive: bool,
    pub latency_ms: u64,
    pub error: Option<String>,
}

#[derive(serde::Serialize)]
pub struct SavePayloadResult {
    pub is_saved_to_disk: bool,
    pub absolute_path: Option<String>,
    pub error: Option<String>,
}

pub fn save_response_payload(
    task_id: &str,
    payload: &str,
    custom_dir: Option<&str>,
) -> SavePayloadResult {
    let mut target_dir = match custom_dir {
        Some(dir) if !dir.trim().is_empty() => PathBuf::from(dir),
        _ => std::env::temp_dir(),
    };

    let file_name = format!(
        "1c_addin_{}_{}.json.gz",
        task_id,
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
    );

    target_dir.push(file_name);

    let file = match File::create(&target_dir) {
        Ok(f) => f,
        Err(e) => {
            return SavePayloadResult {
                is_saved_to_disk: false,
                absolute_path: None,
                error: Some(format!(
                    "Кластер не смог создать файл по пути {:?}: {}",
                    target_dir, e
                )),
            }
        }
    };

    let buf_writer = BufWriter::new(file);

    let mut encoder = GzEncoder::new(buf_writer, Compression::fast());

    if let Err(e) = encoder.write_all(payload.as_bytes()) {
        return SavePayloadResult {
            is_saved_to_disk: false,
            absolute_path: None,
            error: Some(format!("Ошибка закрытия gzip-дескриптора: {}", e)),
        };
    }
    let absolute_path_str = target_dir.to_string_lossy().into_owned();

    SavePayloadResult {
        is_saved_to_disk: true,
        absolute_path: Some(absolute_path_str),
        error: None,
    }
}

pub async fn ping_url(url: &str, timeout_sec: u64) -> PingResult {
    let client = match Client::builder()
        .timeout(Duration::from_secs(timeout_sec))
        .danger_accept_invalid_certs(true)
        .build()
    {
        Ok(c) => c,
        Err(e) => {
            return PingResult {
                is_alive: false,
                latency_ms: 0,
                error: Some(format!("Ошибка создания HTTP-клиента для пинга: {}", e)),
            }
        }
    };
    let start = Instant::now();
    match client.head(url).send().await {
        Ok(response) => {
            let latency = start.elapsed().as_millis() as u64;
            PingResult {
                is_alive: response.status().is_success()
                    || response.status().is_redirection()
                    || response.status().as_u16() >= 400,
                latency_ms: latency,
                error: if response.status().is_success() {
                    None
                } else {
                    Some(format!("Статус ответа: {}", response.status()))
                },
            }
        }
        Err(e) => PingResult {
            is_alive: false,
            latency_ms: start.elapsed().as_millis() as u64,
            error: Some(format!("Хост недоступен: {}", e)),
        },
    }
}

pub fn safe_deserialize_json<'a, T>(json_str: &'a str) -> Result<T, String>
where
    T: serde::Deserialize<'a>,
{
    if json_str.trim().is_empty() {
        return Err("1С передала пустую строку вместо JSON-данных".to_string());
    }
    serde_json::from_str::<T>(json_str).map_err(|e| {
        format!(
            "Ошибка парсинга JSON от 1С: {} (Позиция: линия {}, колонка {})",
            e,
            e.line(),
            e.column()
        )
    })
}

pub fn safe_serialize_json<T>(data: &T) -> String
where
    T: serde::Serialize,
{
    serde_json::to_string(data).unwrap_or_else(|e| {
        format!(
            r#"{{"error": "Критическая ошибка сериализации на стороне Rust: {}"}}"#,
            e
        )
    })
}

pub async fn run_with_retry<F, Fut, O, E>(mut action: F) -> Result<O, E>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<O, E>>,
    E: std::fmt::Display,
{
    let retry_strategy = ExponentialFactorBackoff::from_millis(100, 2.0)
        .map(jitter)
        .take(3);

    let result = Retry::spawn(retry_strategy, move || {
        let fut = action();
        async move {
            match fut.await {
                Ok(val) => Ok(val),
                Err(e) => Err(tokio_retry2::RetryError::transient(e)),
            }
        }
    })
    .await;

    match result {
        Ok(val) => Ok(val),
        Err(retry_error) => Err(retry_error),
    }
}
