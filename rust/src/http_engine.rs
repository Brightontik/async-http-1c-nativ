use crate::cache::{AddInCache, NetworkResult, NetworkTask};
use crate::https::configure_ssl;
use crate::utils::run_with_retry;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time::timeout;

async fn process_task(task: NetworkTask, client: Arc<reqwest::Client>, cache: Arc<AddInCache>) {
    let method =
        reqwest::Method::from_str(&task.metod.to_uppercase()).unwrap_or(reqwest::Method::GET);

    let response_res = run_with_retry(|| {
        let client_clone = client.clone();
        let method_clone = method.clone();
        let url_clone = task.url.clone();
        let headers_clone = task.headers.clone();
        let payload_clone = task.payload.clone();

        async move {
            let mut request_builder = client_clone.request(method_clone, &url_clone);

            for (key, value) in &headers_clone {
                request_builder = request_builder.header(key, value);
            }

            if !payload_clone.is_empty() {
                request_builder = request_builder.body(payload_clone.clone());
            }

            request_builder.send().await
        }
    })
    .await;

    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    let network_result = match response_res {
        Ok(response) => {
            let status_code = response.status().as_u16();
            match response.text().await {
                Ok(body) => NetworkResult {
                    task_id: task.task_id.clone(),
                    service_name: task.service_name.clone(),
                    status_code,
                    response_body: body,
                    error: None,
                    unix_timestamp: timestamp,
                },
                Err(e) => NetworkResult {
                    task_id: task.task_id.clone(),
                    service_name: task.service_name.clone(),
                    status_code,
                    response_body: String::new(),
                    error: Some(format!("Не удалось прочитать тело ответа: {}", e)),
                    unix_timestamp: timestamp,
                },
            }
        }
        Err(e) => {
            let status_code = e.status().map(|s| s.as_u16()).unwrap_or(0);
            NetworkResult {
                task_id: task.task_id.clone(),
                service_name: task.service_name.clone(),
                status_code,
                response_body: String::new(),
                error: Some(format!("Ошибка сетевого запроса: {}", e)),
                unix_timestamp: timestamp,
            }
        }
    };
    cache.output_storage.insert(task.task_id, network_result);
}

pub fn start_http_engine(cache: Arc<AddInCache>) {
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("Не удалось запустить сетевой рантайм Tokio");

        loop {
            match cache.task_receiver.recv() {
                Ok(task) => {
                    let cache_clone = cache.clone();
                    let mut client_builder = reqwest::Client::builder()
                        .timeout(cache.max_duration)
                        .pool_max_idle_per_host(10);

                    client_builder = configure_ssl(client_builder, &cache.ssl_config)
                        .unwrap_or_else(|e| {
                            eprintln!("SSL Config Error: {}", e);
                            reqwest::Client::builder()
                        });

                    let client = Arc::new(
                        client_builder
                            .build()
                            .unwrap_or_else(|_| reqwest::Client::new()),
                    );

                    rt.spawn(async move {
                        process_task(task, client, cache_clone).await;
                    });
                }
                Err(_) => {
                    break;
                }
            }
        }
    });
}
