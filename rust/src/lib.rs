pub mod cache;
pub mod http_engine;
pub mod https;
pub mod parser;
pub mod utils;
use crate::cache::AddInCache;
use native_api_1c::native_api_1c_core::ffi::connection::Connection;
use native_api_1c::native_api_1c_core::ffi::string_utils::{from_os_string, get_str};
use native_api_1c::native_api_1c_core::ffi::{self, AttachType};
use native_api_1c::native_api_1c_macro::AddIn;
use std::fs::OpenOptions;
use std::io::Write;
use std::sync::Arc;

fn log_message(message: &str) {
    let _ = OpenOptions::new()
        .create(true)
        .append(true)
        .open("C:\\tmp\\1c_network_component.log")
        .and_then(|mut file| writeln!(file, "{}", message));
}

#[derive(AddIn)]
#[add_in_prop(name = "NetworkAddIn", ffi = "false")]
pub struct NetworkAddIn {
    #[add_in_con]
    connection: Arc<Option<&'static Connection>>,

    #[add_in_prop(readable, name = "Version", name_ru = "Версия")]
    pub version: String,

    #[add_in_func(name = "Configure", name_ru = "Настроить")]
    #[arg(Str)]
    #[returns(Bool)]
    pub configure: fn(&mut Self, String) -> bool,

    #[add_in_func(name = "PushTask", name_ru = "ДобавитьЗадачу")]
    #[arg(Str)]
    #[returns(Bool)]
    pub push_tasks: fn(&Self, String) -> bool,

    #[add_in_func(name = "PopResults", name_ru = "ЗабратьРезультаты")]
    #[arg(Str)]
    #[arg(Int)]
    #[returns(Str)]
    pub pop_results: fn(&Self, String, i64) -> String,

    #[add_in_func(name = "AckResults", name_ru = "ПодтвердитьПолучение")]
    #[arg(Str)]
    pub ack_results: fn(&Self, String),

    pub cache: Option<Arc<AddInCache>>,
}

impl Default for NetworkAddIn {
    fn default() -> Self {
        log_message("NetworkAddIn::default() конструктор вызван");
        Self {
            connection: std::sync::Arc::new(None),
            version: "1.0.0".to_string(),
            configure: Self::ffi_configure,
            push_tasks: Self::ffi_push_tasks,
            pop_results: Self::ffi_pop_results,
            ack_results: Self::ffi_ack_results,
            cache: None,
        }
    }
}

impl NetworkAddIn {
    pub fn new() -> Self {
        Self {
            connection: std::sync::Arc::new(None),
            version: "1.0.0".to_string(),
            configure: Self::ffi_configure,
            push_tasks: Self::ffi_push_tasks,
            pop_results: Self::ffi_pop_results,
            ack_results: Self::ffi_ack_results,
            cache: None,
        }
    }

    fn get_cache(&self) -> Result<&Arc<AddInCache>, String> {
        match &self.cache {
            Some(c) => Ok(c),
            None => {
                let err_msg = "Критическая ошибка: Движок не инициализирован! Сначала необходимо вызвать метод Настроить().";
                log_message(err_msg);
                Err(err_msg.to_string())
            }
        }
    }

    fn ffi_configure(&mut self, json_settings: String) -> bool {
        log_message(&format!("ffi_configure called with: {}", json_settings));
        let settings = match crate::utils::safe_deserialize_json::<
            crate::parser::AddInSettingsManifest,
        >(&json_settings)
        {
            Ok(s) => s,
            Err(e) => {
                log_message(&format!("Ошибка парсинга настроек: {}", e));
                return false;
            }
        };

        let ssl_config = crate::https::SSLConfig {
            danger_accept_invalid_certs: settings.danger_accept_invalid_certs,
            ca_cert_path: settings.ca_cert_path,
            client_cert_path: settings.client_cert_path,
            client_key_path: settings.client_key_path,
        };

        let cache_obj = AddInCache::new(settings.max_rows, settings.secs_timeouts, ssl_config);

        let cache_arc = Arc::new(cache_obj);

        crate::http_engine::start_http_engine(cache_arc.clone());

        self.cache = Some(cache_arc);

        log_message("Сетевой движок успешно настроен и запущен в фоне");
        true
    }

    fn ffi_push_tasks(&self, json_task: String) -> bool {
        log_message("ffi_push_tasks called");

        let cache = match self.get_cache() {
            Ok(c) => c,
            Err(_) => return false,
        };

        let tasks = match crate::parser::process_incoming_json(&json_task) {
            Ok(t) => t,
            Err(e) => {
                log_message(&format!("Ошибка парсинга пакета задач от 1С: {}", e));
                return false;
            }
        };

        log_message(&format!(
            "Успешно распарсено задач: {}. Начинаем заброс в очередь...",
            tasks.len()
        ));

        for task in tasks {
            match cache.push_task(task) {
                crate::cache::PushResult::Ok => {}
                crate::cache::PushResult::QueueFull => {
                    log_message("Предупреждение: Входящая очередь задач переполнена (QueueFull)!");
                }
                crate::cache::PushResult::RuntimeDisconnected => {
                    log_message("Критическая ошибка: Сетевой рантайм отключился от очереди (RuntimeDisconnected)!");
                    return false;
                }
            }
        }
        log_message("Все задачи успешно переданы в фоновый конвейер");
        true
    }

    fn ffi_pop_results(&self, filter_service: String, limit: i64) -> String {
        log_message(&format!(
            "ffi_pop_results called with filter='{}', limit={}",
            filter_service, limit
        ));

        let cache = match self.get_cache() {
            Ok(c) => c,
            Err(e) => return crate::utils::safe_serialize_json(&format!("Ошибка: {}", e)),
        };

        let filter = if filter_service.trim().is_empty() {
            None
        } else {
            Some(filter_service.as_str())
        };

        let max_limit = if limit <= 0 { 0 } else { limit as usize };

        let results = cache.pop_results(filter, max_limit);
        log_message(&format!(
            "Извлечено готовых результатов из кэша: {}",
            results.len()
        ));

        crate::utils::safe_serialize_json(&results)
    }

    fn ffi_ack_results(&self, json_task_ids: String) {
        log_message(&format!(
            "ffi_ack_results called with parameters: {}",
            json_task_ids
        ));
        let cache = match self.get_cache() {
            Ok(c) => c,
            Err(_) => return,
        };
        let task_ids: Vec<String> = match crate::utils::safe_deserialize_json(&json_task_ids) {
            Ok(ids) => ids,
            Err(e) => {
                log_message(&format!(
                    "Ошибка парсинга массива task_id для подтверждения: {}",
                    e
                ));
                return;
            }
        };
        log_message(&format!(
            "Получено подтверждение для {} задач. Очищаем ОЗУ...",
            task_ids.len()
        ));

        cache.ack_result(task_ids);

        log_message("Очистка кэша результатов успешно завершена");
    }
}
