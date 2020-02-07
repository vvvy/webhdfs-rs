use std::collections::HashMap;
use std::sync::Arc;
use http::{Uri, uri::Authority};

use crate::error::*;

pub struct NatMap {
    natmap: HashMap<String, Authority>
}

impl NatMap {
    pub fn new(mut src: impl Iterator<Item=(String, String)>) -> Result<NatMap> {
        src
        .try_fold(
            HashMap::new(), 
            |mut m, (k, v)| v.parse().aerr_f(|| format!("cannot parse NAT value for k={}", k)).map(|v| { m.insert(k, v); m } )
        ).map(|natmap| NatMap { natmap })
    }
    pub fn translate(&self, uri: Uri) -> Result<Uri> {
        if self.natmap.is_empty() {
            Ok(uri)
        } else {
            if let Some(s) = uri.authority() {
                if let Some(replacement) = self.natmap.get(s.as_str()) {
                    let mut parts = uri.into_parts();
                    parts.authority = Some(replacement.clone());
                    Ok(http::uri::Uri::from_parts(parts).aerr("Could not assemble redirect uri after NAT")?)
                } else {
                    Ok(uri)
                }        
            } else { 
                Ok(uri) 
            }
        }
    }
}

#[derive(Clone)]
pub struct NatMapPtr {
    ptr: Option<Arc<NatMap>>
}

impl NatMapPtr {
    pub fn new(natmap: NatMap) -> NatMapPtr {
        NatMapPtr { ptr: if natmap.natmap.is_empty() { None } else { Some(Arc::new(natmap)) } }
    }

    pub fn empty() -> NatMapPtr {
        NatMapPtr { ptr: None }
    }

    pub fn translate(&self, uri: Uri) -> Result<Uri> {
        if let Some(p) = &self.ptr {
            p.translate(uri)
        } else {
            Ok(uri)
        }
    }
}
