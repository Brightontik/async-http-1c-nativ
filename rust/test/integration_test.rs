use network_engine::cache::{AddInCache, NetworkResult, NetworkTask, PushResult};
use network_engine::https::SSLConfig;
use network_engine::parser::{process_incoming_json, AddInSettingsManifest};
use network_engine::utils::{safe_deserialize_json, save_response_payload};
use network_engine::NetworkAddIn;
use std::collections::HashMap;
use std::default::Default;
use std::fs;
use std::path::Path;
use std::thread;
use std::time::Duration;

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_cache_delivery_and_ack_pipeline() {
        // 1. Инициализируем пустой конфиг SSL для теста
        let ssl_config = SSLConfig {
            danger_accept_invalid_certs: true,
            ca_cert_path: None,
            client_cert_path: None,
            client_key_path: None,
        };

        // 2. Создаем кэш: макс 10 строк, таймаут 30 секунд
        let cache = AddInCache::new(10, 30, ssl_config);

        // 3. Создаем тестовую сетевую задачу
        let task = NetworkTask {
            task_id: "test_uuid_001".to_string(),
            service_name: "TestService".to_string(),
            url: "https://httpbin.org".to_string(),
            metod: "GET".to_string(),
            headers: HashMap::new(),
            payload: String::new(),
        };

        // 4. Тестируем заброс в очередь (Стадия 1)
        let push_res = cache.push_task(task);
        assert_eq!(push_res, PushResult::Ok);

        // 5. Симулируем фоновое завершение задачи:
        // Руками пишем результат в output_storage, как это делает http_engine
        let mock_result = NetworkResult {
            task_id: "test_uuid_001".to_string(),
            service_name: "TestService".to_string(),
            status_code: 200,
            response_body: r#"{"status": "success"}"#.to_string(),
            error: None,
            unix_timestamp: 1700000000,
        };
        cache
            .output_storage
            .insert("test_uuid_001".to_string(), mock_result);

        // 6. 1С запрашивает результаты (Первое чтение)
        let first_pop = cache.pop_results(Some("TestService"), 10);
        assert_eq!(first_pop.len(), 1, "Должен вернуться ровно 1 результат");
        assert_eq!(first_pop[0].status_code, 200);

        // 7. Проверяем неразрушающее чтение (At-Least-Once):
        // Данные НЕ должны удаляться из кэша до ACK-подтверждения!
        let second_pop = cache.pop_results(Some("TestService"), 10);
        assert_eq!(
            second_pop.len(),
            1,
            "Данные пропали из кэша до подтверждения 1С!"
        );

        // 8. 1С успешно записала данные в базу и шлет ACK (Рукопожатие)
        cache.ack_result(vec!["test_uuid_001".to_string()]);

        // 9. Проверяем, что ОЗУ очистилось
        let third_pop = cache.pop_results(Some("TestService"), 10);
        assert_eq!(third_pop.len(), 0, "Кэш не очистился после команды ACK!");
    }
}

/// 1. НЕГАТИВНЫЙ И ПОЗИТИВНЫЙ ТЕСТ ПАРСЕРА ЗАДАЧ
#[test]
fn test_parser_resilience() {
    // Сценарий А: Полностью битый JSON. Обязан вернуть Err, а не упасть в панику!
    let broken_json = r#"[{"task_id": "1", "url": "broken..."#;
    let res = process_incoming_json(broken_json);
    assert!(
        res.is_err(),
        "Парсер должен был вернуть ошибку на кривой JSON"
    );

    let err_msg = res.unwrap_err();
    assert!(
        err_msg.contains("Ошибка парсинга JSON от 1С"),
        "Ошибка должна быть информативной"
    );

    // Сценарий Б: Минималистичный JSON (1С-ник передал только обязательные поля).
    // Тестируем, что #[serde(default)] в manifest.rs подставит каноничные дефолты.
    let minimal_json = r#"{
        "tasks": [
            {
                "task_id": "req_min_001",
                "service_name": "Test",
                "url": "https://ya.ru"
            }
        ]
    }"#;

    let tasks_res = process_incoming_json(minimal_json);
    assert!(
        tasks_res.is_ok(),
        "Парсер должен прощать отсутствие необязательных полей"
    );

    let tasks = tasks_res.unwrap();
    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks[0].metod, "GET", "Дефолтный метод должен быть GET");
    assert!(
        tasks[0].headers.is_empty(),
        "Заголовки по дефолту должны быть пустыми"
    );
    assert!(
        tasks[0].payload.is_empty(),
        "Тело по дефолту должно быть пустым"
    );
}

/// 2. ТЕСТ ДЕФОЛТОВ НАСТРОЕК
#[test]
fn test_settings_defaults() {
    // Передаем пустой JSON-объект. Проверяем, что подставятся наши внутренние лимиты
    let empty_settings_json = r#"{}"#;
    let res = safe_deserialize_json::<AddInSettingsManifest>(empty_settings_json);

    assert!(res.is_ok());
    let settings = res.unwrap();
    assert_eq!(settings.max_rows, 1000, "Дефолт очереди должен быть 1000");
    assert_eq!(
        settings.secs_timeouts, 30,
        "Дефолт таймаута должен быть 30 секунд"
    );
    assert_eq!(
        settings.danger_accept_invalid_certs, false,
        "SSL-проверки по дефолту должны быть включены"
    );
}

/// 3. ТЕСТ ЗАЩИТЫ ОТ ПЕРЕПОЛНЕНИЯ ПАМЯТИ (OOM / СЖАТИЕ НА ДИСК)
#[test]
fn test_oom_protection_and_gzip() {
    // Генерируем "тяжелый" ответ в 1 Мегабайт из повторяющихся символов
    let heavy_payload = "A".repeat(1024 * 1024);

    // Сбрасываем во временную папку ОС (custom_dir = None)
    let save_res = save_response_payload("oom_test_id", &heavy_payload, None);

    assert!(
        save_res.is_saved_to_disk,
        "Файл должен успешно записаться на диск"
    );
    assert!(save_res.absolute_path.is_some());
    assert!(save_res.error.is_none());

    let file_path_str = save_res.absolute_path.unwrap();
    let file_path = Path::new(&file_path_str);

    // Проверяем, что файл физически создан
    assert!(file_path.exists(), "Архив должен существовать на диске");

    // Философская проверка: сжатый Gzip должен весить в разы МЕНЬШЕ, чем 1 МБ исходного текста
    let metadata = fs::metadata(file_path).unwrap();
    let compressed_size = metadata.len();

    assert!(
        compressed_size < 50 * 1024,
        "Gzip должен был сжать 1МБ одинаковых букв до пары десятков килобайт. Фактический размер: {} байт", 
        compressed_size
    );

    // Подчищаем за собой тестовый файл, чтобы не мусорить в системе
    let _ = fs::remove_file(file_path);
}

#[test]
fn test_live_combat_parallel_requests() {
    // 1. Имитируем JSON настроек от 1С (очередь 50, таймаут 5 сек, без SSL проверок)
    let settings_json = r#"{
        "max_rows": 50,
        "secs_timeouts": 5,
        "danger_accept_invalid_certs": true
    }"#;

    // 2. Создаем и настраиваем нашу компоненту (как при Новый("AddIn..."))
    let mut addin = NetworkAddIn::default();
    let config_res: bool = (addin.configure)(&mut addin, settings_json.to_string());
    assert!(config_res, "Компонента должна успешно настроиться");

    // 3. Формируем пакет из 10 параллельных задач для 1С
    // 9 запросов пойдут на реальный сервер с правильными параметрами, а 10-й запрос — на несуществующий домен
    let mut tasks_json = String::from(r#"{"tasks": ["#);

    for i in 1..=9 {
        tasks_json.push_str(&format!(
            r#"{{"task_id": "req_combat_{}", "service_name": "CombatTest", "url": "https:////ya.ru", "method": "GET"}},"#,
            i
        ));
    }
    // Добавляем 10-ю проблемную задачу с битым DNS для проверки изоляции ошибок
    tasks_json.push_str(
        r#"{"task_id": "req_combat_10", "service_name": "CombatTest", "url": "https://this-domain-dead-12345.org", "method": "GET"}"#
    );
    tasks_json.push_str(r#"]}"#);

    // 4. Имитируем мгновенный заброс со стороны 1С (ВК сразу возвращает true)
    let push_res: bool = (addin.push_tasks)(&addin, tasks_json);
    assert!(push_res, "Пакет задач должен мгновенно улететь в очередь");

    // 5. Вместо жёсткого sleep делаем динамический пуллинг результатов (до 10 секунд максимум)
    let mut results_json = String::new();
    let mut attempts = 0;

    println!("Начинаем динамическое ожидание ответов от Tokio...");

    while attempts < 20 {
        // 20 попыток по 500мс = 10 секунд максимум
        thread::sleep(Duration::from_millis(500));

        // Забираем результаты без лимита (100 строк), чтобы оценить наполнение кэша
        results_json = (addin.pop_results)(&addin, "CombatTest".to_string(), 100);

        // Если в JSON пришло что-то кроме пустого массива "[]", проверяем количество
        if results_json != "[]" {
            let current_results: Vec<serde_json::Value> =
                serde_json::from_str(&results_json).unwrap();
            // Ждём, пока в кэш упадут все 10 задач (9 успешных + 1 битая)
            if current_results.len() == 10 {
                println!("Все 10 задач успешно обработаны на попытке №{}", attempts);
                break;
            }
        }
        attempts += 1;
    }

    // 6. Безопасно парсим финальный JSON-массив результатов
    let results: Vec<serde_json::Value> = serde_json::from_str(&results_json)
        .expect("ВК должна была вернуть валидный JSON-массив результатов");

    // ====================================================================
    // 🔍 НАШ ЦЕНТРАЛЬНЫЙ ДАМП ДЛЯ ВИЗУАЛЬНОГО АУДИТА:
    println!("\n=================== [ ДАМП ОТВЕТОВ ОТ В К ДЛЯ 1С ] ===================");
    println!("{:#?}", results); // Выведет весь массив структур с отступами и подсветкой
    println!("======================================================================\n");
    // ====================================================================

    // Защитная проверка: если за 10 секунд сеть вообще не ответила, выводим дамп
    assert_eq!(
        results.len(),
        10,
        "За 10 секунд рантайм не успел собрать все 10 ответов. Вернулось: {}. JSON: {}",
        results.len(),
        results_json
    );

    // 7. Проверяем внутренности ответов, чтобы убедиться, что сеть реально отработала
    assert_eq!(
        results.len(),
        10,
        "В кэше должны быть результаты по всем 10 задачам"
    );

    for res in results {
        let task_id = res["task_id"].as_str().unwrap_or("");
        let status = res["status_code"].as_u64().unwrap_or(0);
        let error_field = res["error"].as_str();

        // Проверяем, что задача не пустая и содержит хоть какую-то информацию от конвейера
        assert!(
            !task_id.is_empty(),
            "Каждая задача должна содержать task_id"
        );

        if let Some(err_text) = error_field {
            // Если сеть упала, проверяем, что ошибка вменяемая
            println!(
                "Задача {} завершилась с сетевой ошибкой: {}",
                task_id, err_text
            );
            assert!(
                err_text.contains("Ошибка")
                    || err_text.contains("failed")
                    || err_text.contains("connection"),
                "Текст ошибки должен быть информативным"
            );
        } else {
            // Если сеть прошла, проверяем HTTP статус
            println!(
                "Задача {} успешно выполнена, HTTP статус: {}",
                task_id, status
            );
            assert!(
                status > 0,
                "При успешном запросе статус-код должен быть заполнен"
            );
        }
    }

    println!("Все проверки структуры ответов успешно пройдены!");

    // 8. Финализируем конвейер: 1С шлет ACK для очистки памяти
    let mut ids_to_ack = Vec::new();
    for i in 1..=10 {
        ids_to_ack.push(format!("req_combat_{}", i));
    }
    let ack_json = serde_json::to_string(&ids_to_ack).unwrap();
    (addin.ack_results)(&addin, ack_json);

    // 9. Проверяем, что после ACK ОЗУ чистое
    let empty_results_json = (addin.pop_results)(&addin, "CombatTest".to_string(), 100);
    assert_eq!(
        empty_results_json, "[]",
        "После ACK-подтверждения кэш результатов обязан очиститься"
    );
}
