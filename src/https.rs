use hyper::client::HttpConnector;
use hyper_tls::HttpsConnector;
use native_tls::{TlsConnector, Identity, Protocol, Certificate};
use crate::config::HttpsConfig;
use crate::error::*;

pub type HttpsConnectorType = HttpsConnector<HttpConnector>;

pub struct HttpsSettings {
    hc: HttpsConnectorType
}

impl From<HttpsConfig> for HttpsSettings {
    fn from(config: HttpsConfig) -> HttpsSettings {
        https_settings_from_config_f(config).unwrap_or_else(|e| panic!("https settings failure: {}", e))
    }
}

pub type HttpsSettingsPtr = std::rc::Rc<HttpsSettings>;

#[inline]
pub fn https_settings_ptr(https_settings: HttpsSettings) -> HttpsSettingsPtr {
    std::rc::Rc::new(https_settings)
}

pub fn https_connector(cfg: &HttpsSettingsPtr) -> HttpsConnectorType {
    cfg.hc.clone()
}

fn _test_types() {
    fn is_clone<T: Clone>() { }
    is_clone::<HttpsConnectorType>();
}


pub fn read_identity_file(file_path: &str, password: &str) -> Result<Identity> {
    use std::io::Read;
    let mut file_data = vec![];
    let _ = std::fs::File::open(file_path)?.read_to_end(&mut file_data)?;
    let r = Identity::from_pkcs12(&file_data, password)?;
    Ok(r)
}

pub fn read_cert_file(file_path: &str) -> Result<Certificate> {
    use std::io::Read;
    let mut file_data = vec![];
    let _ = std::fs::File::open(file_path)?.read_to_end(&mut file_data)?;
    let r = Certificate::from_der(&file_data)?;
    Ok(r)
}

/// fallible version of convert_https_settings
fn https_settings_from_config_f(config: HttpsConfig) -> Result<HttpsSettings> {
    let identity_password: &str = if let Some(s) = &config.identity_password { &s } else { "" };

    fn pv(s: String) -> Result<Option<Protocol>> {
        match s.as_ref() {
            "Sslv3" => Ok(Some(Protocol::Sslv3)),
            "Tlsv10" => Ok(Some(Protocol::Tlsv10)),
            "Tlsv11" => Ok(Some(Protocol::Tlsv11)),
            "Tlsv12" => Ok(Some(Protocol::Tlsv12)),
            "no_check" => Ok(None),
            other => Err(app_error!(generic "Invalid TLS protocol version setting '{}'", other))
        }
    }

    let mut cb = TlsConnector::builder();
    if let Some(w) = config.danger_accept_invalid_certs { cb.danger_accept_invalid_certs(w); }
    if let Some(w) = config.danger_accept_invalid_hostnames { cb.danger_accept_invalid_hostnames(w); }
    if let Some(w) = config.use_sni { cb.use_sni(w); }
    if let Some(w) = config.min_protocol_version { cb.min_protocol_version(pv(w)?); }
    if let Some(w) = config.max_protocol_version { cb.max_protocol_version(pv(w)?); }
    if let Some(w) = config.identity_file { 
        cb.identity(read_identity_file(&w,identity_password).aerr_f(|| format!("read_identity_file({}): error", &w))?);
    }
    if let Some(w) = config.root_certificates { for c in w { cb.add_root_certificate(read_cert_file(&c)?); } }
    let tc = cb.build().unwrap_or_else(|e| panic!("HttpsConnector::new() failure: {}", e));
    let mut httpc = HttpConnector::new();
    httpc.enforce_http(false);
    let hc: HttpsConnectorType = (httpc, tc.into()).into();
    Ok(HttpsSettings { hc })
}
