#[macro_use] 
mod error;
mod rest_client;
mod datatypes;
mod natmap;
mod uri_tools;

use std::cell::RefCell;

use http::{Uri, uri::Parts as UriParts};

use tokio::runtime::Runtime;
use futures::Future;

use error::*;
use rest_client::HttpxClient;
use uri_tools::*;
use datatypes::*;

pub use natmap::{NatMap, NatMapPtr};

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


pub fn dir_async(cx: &HdfsContext, path: &str) -> impl Future<Item=ListStatusResponse, Error=Error> {
    let natmap = cx.natmap.clone();
    futures::future::result(cx.uri(path, Op::LISTSTATUS, vec![])).and_then(|uri| HttpxClient::get_with_redirect(uri, natmap))
}

pub fn dir(cx: &HdfsContext, path: &str) -> Result<ListStatusResponse> {
    //let uri = cx.uri(path, Op::LISTSTATUS, vec![])?;
    //let future = get_with_redirect(uri, cx.natmap.clone());
    cx.rt.borrow_mut().block_on(dir_async(cx, path))
}
