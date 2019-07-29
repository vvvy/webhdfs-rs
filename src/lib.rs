#[macro_use] 
mod error;
mod rest_client;
mod datatypes;
mod uri_tools;

//use std::io::{Read, Seek, SeekFrom};
//use std::io::Result as IoResult;
use std::collections::HashMap;
use std::sync::Arc;
use std::cell::RefCell;

//use serde::{Serialize, Deserialize};

use http::{Uri, uri::Parts as UriParts, uri::Authority};

use tokio::runtime::Runtime;
use futures::Future;

use error::*;
use rest_client::HttpxClient;
use uri_tools::*;
use datatypes::*;



#[derive(Debug)]
pub enum Op {
    LISTSTATUS
}

impl Op {
    pub fn op_string(&self) -> &'static str {
        match self {
            Op::LISTSTATUS => "LISTSTATUS"
        }
    }
}

#[derive(Debug)]
pub enum OpArg {
    Length(i64)
}

impl OpArg {
    fn add_to_url(&self, qe: QueryEncoder) -> QueryEncoder {
        match self {
            OpArg::Length(l) => qe.add_pi("length", *l)
        }
    }
}

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
            if let Some(s) = uri.authority_part() {
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


pub struct HdfsContext {
    entrypoint: UriParts,
    natmap: NatMapPtr,
    rt: RefCell<Runtime>
}

impl HdfsContext {
    const SVC_MOUNT_POINT: &'static str = "/webhdfs/v1";

    pub fn new(entrypoint: Uri, natmap: NatMap) -> Result<HdfsContext> {
        Ok(Self { entrypoint: entrypoint.into_parts(), natmap: NatMapPtr::new(natmap), rt: RefCell::new(Runtime::new()?) })
    }
    pub fn from_entrypoint(entrypoint: Uri) -> Result<HdfsContext> {
        Ok(Self { entrypoint: entrypoint.into_parts(), natmap: NatMapPtr::empty(), rt: RefCell::new(Runtime::new()?) })
    }
    fn uri(&self, file_path: &str, op: Op, args: Vec<OpArg>) -> Result<Uri> {
        let mut b = Uri::builder();        
        if let Some(scheme) = &self.entrypoint.scheme { b.scheme(scheme.clone()); }
        if let Some(authority) = &self.entrypoint.authority { b.authority(authority.clone()); }

        let q0 = PathEncoder::new(Self::SVC_MOUNT_POINT).extend(file_path).query();
        let q1 = q0.add_pv("op", op.op_string());
        let q2 = args.iter().fold(q1, |q, s| s.add_to_url(q));
        let p = q2.result();

        b.path_and_query(&p as &[u8]).build()
        .aerr_f(|| format!("Could not build URI: file_path={}, op={:?}, args={:?}", file_path, op, args))
    }
}

/*
pub struct ReadHdfsFile {
    cx: HdfsContext,
    len: u64,
    pos: u64
}

impl ReadHdfsFile {
    pub fn create(cx: HdfsContext, path: &str) -> IoResult<ReadHdfsFile> {
        unimplemented!()
    }
    pub fn len(&self) -> u64 { self.len }
}

impl Read for ReadHdfsFile {
    fn read(&mut self, buf: &mut [u8]) -> IoResult<usize> {
        unimplemented!()
    }
}

impl Seek for ReadHdfsFile {
    fn seek(&mut self, pos: SeekFrom) -> IoResult<u64> {
        unimplemented!()
    }
}
*/

fn get_with_redirect<R>(uri: Uri, natmap: NatMapPtr)  -> impl Future<Item=R, Error=Error> + Send
where R: serde::de::DeserializeOwned + Send + 'static {

    fn do_get<R>(uri: Uri) -> Box<dyn Future<Item=R, Error=Error> + Send> 
    where R: serde::de::DeserializeOwned + Send + 'static {
        match HttpxClient::new(&uri) {
            Ok(c) => Box::new(c.get::<R>(uri)),
            Err(e) => Box::new(futures::future::err(e))
        }
    }

    fn handle_redirect<R>(r: Result<R>, natmap: NatMapPtr) -> Box<dyn Future<Item=R, Error=Error> + Send>
    where R: serde::de::DeserializeOwned + Send + 'static {
        use futures::future::{ok, err};
        match r {
            Ok(r) => Box::new(ok(r)),
            Err(e) => match e.to_http_redirect() {
                Ok((_code, location)) => match location.parse() {
                    Ok(uri) => match natmap.translate(uri) { 
                        Ok(uri) => do_get(uri),
                        Err(e) => Box::new(err(e))
                    }
                    Err(e) => Box::new(err(app_error!((cause=e) "Cannot parse location URI returned by redirect")))
                }
                Err(e) => Box::new(err(e))
            }
        }
    }

    do_get::<R>(uri).then(|r| handle_redirect(r, natmap))
}


pub fn dir_async(cx: &HdfsContext, path: &str) -> impl Future<Item=ListStatusResponse, Error=Error> {
    let natmap = cx.natmap.clone();
    futures::future::result(cx.uri(path, Op::LISTSTATUS, vec![])).and_then(|uri| get_with_redirect(uri, natmap))
}

pub fn dir(cx: &HdfsContext, path: &str) -> Result<ListStatusResponse> {
    //let uri = cx.uri(path, Op::LISTSTATUS, vec![])?;
    //let future = get_with_redirect(uri, cx.natmap.clone());
    cx.rt.borrow_mut().block_on(dir_async(cx, path))
}
