//! File-based configuration.
//! 
//! 1. Confiuration is read from one of the configuration files when `HdfsClientBuilder::from_config` or 
//!    `HdfsClientBuilder::from_config` is called. The configuration files are never read or used unless 
//!    explicitly requested.
//! 2. There are 3 locations where the library looks for the configuration information, in the order as 
//!    listed below. The search is stopped on first file found, and this solely file is used. No configuration
//!    merging is currently supported.
//!    - If 'WEBHDFS_CONFIG' environment variable is set, then the location specfied by it is opened
//!      (must be a full file path). The library panics if there is no file at that path.
//!    - A file named 'webhdfs.toml' in the CWD.
//!    - A file named '.webhdfs.toml' in the user's home directory.
//! 3. If a file is found but is either unreadable or unparseable, then the library panics.
//! 4. All the configuration fields are optional, except for the entrypoint.
//! 5. Use `write_sample_config` to get config sample
//! 
use std::fs::read;
use std::path::Path;
use std::time::Duration;
use http::Uri;

use crate::error::*;
use serde::{Serialize, Serializer, Deserialize, Deserializer};

#[derive(Debug)]
pub struct UriW {
    uri: Uri
}

impl UriW {
    pub fn new(uri: Uri) -> Self { Self { uri } }
    pub fn into_uri(self) -> Uri { self.uri }
}

impl<'de> Deserialize<'de> for UriW {
    fn deserialize<D>(deserializer: D) -> StdResult<Self, D::Error>
    where D: Deserializer<'de> {
        let s: String = Deserialize::deserialize(deserializer)?;
        let uri: Uri = s.parse().map_err(serde::de::Error::custom)?;
        Ok(UriW { uri })
    }
}

impl Serialize for UriW {
    fn serialize<S>(&self, serializer: S) -> StdResult<S::Ok, S::Error>
    where S: Serializer {
        serializer.serialize_str(&self.uri.to_string())
    }
}


#[derive(Serialize, Deserialize, Debug)]
pub struct Config {
    pub entrypoint: UriW,
    pub default_timeout: Option<Duration>,
    pub user_name: Option<String>,
    pub doas: Option<String>,
    pub dt: Option<String>
}

#[cfg(windows)]
#[inline]
fn get_home_dir() -> Option<String> {
    let d: Option<String> = std::env::var("HOMEDRIVE").ok();
    let p: Option<String> = std::env::var("HOMEPATH").ok();
    d.and_then(|d| p.map(|p| d + &p))
}

#[cfg(not(windows))]
#[inline]
fn get_home_dir() -> Option<String> {
    std::env::var("HOME").ok()
}

fn read_local_config() -> Result<Option<Config>> {
    let p = Path::new("webhdfs.toml");
    if p.is_file() {
        Ok(Some(toml::from_slice(&read(p)?)?))
    } else {
        Ok(None)
    }
}

fn read_user_config() -> Result<Option<Config>> {
    match get_home_dir() {
        None => Ok(None),
        Some(f) => {
            let p = Path::new(&f);
            let p = p.join(Path::new(".webhdfs.toml"));
            if p.is_file() {
                Ok(Some(toml::from_slice(&read(p)?)?))
            } else {
                Ok(None)
            }
        }
    }
}

fn read_env_config() -> Result<Option<Config>> {
    match std::env::var("WEBHDFS_CONFIG").ok() {
        None => Ok(None),
        Some(f) => {
            let p = Path::new(&f);
            Ok(Some(toml::from_slice(&read(p)?)?))
        }
    }
}

pub fn read_config() -> Config {
    read_env_config().expect("Configuration error (file specified by WEBHDFS_CONFIG environment var)")
    .or(read_local_config().expect("Configuration error (webhdfs.toml in CWD)"))
    .or(read_user_config().expect("Configuration error (.webhdfs.toml in homedir)"))
    .expect("No valid configuration file has been found")
}

pub fn write_config(path: &Path, c: &Config, new_file: bool) {
    use std::io::Write;
    let mut f = std::fs::OpenOptions::new()
        .write(true)
        .truncate(true)
        .create_new(new_file)
        .open(path)
        .unwrap();
    f.write_all(&toml::to_vec(c).unwrap()).unwrap();
}

pub fn write_sample_config() {
    let c = Config {
        entrypoint: UriW::new("http://namenode.hdfs.intra:50070".parse().unwrap()),
        default_timeout: Some(Duration::from_secs(30)),
        user_name: Some("dr.who".to_owned()),
        doas: Some("doas.user".to_owned()),
        dt: Some("---encoded-delegation-token---".to_owned())
    };
    write_config(&Path::new("template.webhdfs.toml"), &c, false)
}