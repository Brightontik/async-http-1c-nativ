use std::fs::File;
use std::io::Read;
use std::path::Path;

use reqwest::retry::Builder;

pub struct SSLConfig {
    pub danger_accept_invalid_certs: bool,
    pub ca_cert_path: Option<String>,
    pub client_cert_path: Option<String>,
    pub client_key_path: Option<String>,
}

pub fn configure_ssl(
    mut builder: reqwest::ClientBuilder,
    config: &SSLConfig,
) -> Result<reqwest::ClientBuilder, String> {
    if config.danger_accept_invalid_certs {
        builder = builder.danger_accept_invalid_certs(true);
    }

    if let Some(ref ca_path) = config.ca_cert_path {
        let cert_bytes = read_file_bytes(ca_path)?;
        let cert = reqwest::Certificate::from_pem(&cert_bytes)
            .map_err(|e| format!("Ошибка парсинга CA сертификата: {}", e))?;
        builder = builder.add_root_certificate(cert);
    }

    if let Some(ref cert_path) = config.client_cert_path {
        let mut cert_bytes = read_file_bytes(cert_path)?;
        if let Some(ref key_path) = config.client_key_path {
            let mut key_bytes = read_file_bytes(key_path)?;
            let identity = reqwest::Identity::from_pkcs8_pem(&cert_bytes, &key_bytes)
                .map_err(|e| format!("Ошибка создания Identity (mTLS): {}", e))?;
            builder = builder.identity(identity)
        } else {
            return Err("Указан клиентский сертификат, но отсутствует закрытый ключ!".to_string());
        }
    }
    Ok(builder)
}

fn read_file_bytes<P: AsRef<Path>>(path: P) -> Result<Vec<u8>, String> {
    let mut file = File::open(&path)
        .map_err(|e| format!("Не удалось открыть файл {:?}: {}", path.as_ref(), e))?;
    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer)
        .map_err(|e| format!("Не удалось прочитать файл {:?}: {}", path.as_ref(), e))?;
    Ok(buffer)
}
