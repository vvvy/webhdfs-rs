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
    LISTSTATUS,
    GETFILESTATUS,
    OPEN
}

impl Op {
    pub fn op_string(&self) -> &'static str {
        match self {
            Op::LISTSTATUS => "LISTSTATUS",
            Op::GETFILESTATUS => "GETFILESTATUS",
            Op::OPEN => "OPEN"
        }
    }
}

#[derive(Debug)]
pub enum OpArg {
    Offset(i64),
    Length(i64),
    BufferSize(i32)
}

impl OpArg {
    fn add_to_url(&self, qe: QueryEncoder) -> QueryEncoder {
        match self {
            OpArg::Offset(o) => qe.add_pi("offset", *o),
            OpArg::Length(l) => qe.add_pi("length", *l),
            OpArg::BufferSize(z) => qe.add_pi("buffersize", *z as i64)
        }
    }
}

/// HDFS Connection data, etc.
/// TODO split this into async and sync-specific parts 
pub struct HdfsContext {
    entrypoint: UriParts,
    natmap: NatMapPtr,
    rt: RefCell<Runtime>,
    default_timeout_s: u64
}

impl HdfsContext {
    const SVC_MOUNT_POINT: &'static str = "/webhdfs/v1";
    const DEFAULT_TIMEOUT_S: u64 = 30;

    pub fn new(entrypoint: Uri, natmap: NatMap) -> Result<HdfsContext> {
        Ok(Self { 
            entrypoint: entrypoint.into_parts(), 
            natmap: NatMapPtr::new(natmap), 
            rt: RefCell::new(Runtime::new()?),
            default_timeout_s: Self::DEFAULT_TIMEOUT_S
        })
    }
    pub fn from_entrypoint(entrypoint: Uri) -> Result<HdfsContext> {
        Ok(Self { 
            entrypoint: entrypoint.into_parts(), 
            natmap: NatMapPtr::empty(), 
            rt: RefCell::new(Runtime::new()?),
            default_timeout_s: Self::DEFAULT_TIMEOUT_S
        })
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

fn with_timeout<R>(f: impl Future<Item=R, Error=Error>, timeout: u64) -> impl Future<Item=R, Error=Error> {
    use std::time::Duration;
    use tokio::prelude::FutureExt;
    f.timeout(Duration::from_secs(timeout)).map_err(|e| match e.into_inner() {
        Some(e) => e,
        None => Error::timeout_c("HTTP operation timed out")
    })
}

pub fn dir_async(cx: &HdfsContext, path: &str) -> impl Future<Item=ListStatusResponse, Error=Error> {
    let natmap = cx.natmap.clone();
    futures::future::result(cx.uri(path, Op::LISTSTATUS, vec![]))
        .and_then(|uri| HttpxClient::new_get_json(uri, natmap))
}

pub fn stat_async(cx: &HdfsContext, path: &str) -> impl Future<Item=FileStatusResponse, Error=Error> {
    let natmap = cx.natmap.clone();
    futures::future::result(cx.uri(path, Op::GETFILESTATUS, vec![]))
        .and_then(|uri| HttpxClient::new_get_json(uri, natmap))
}

pub fn file_read_async(cx: &HdfsContext, path: &str, offset: Option<i64>, length: Option<i64>, buffersize: Option<i32>) 
-> impl Stream<Item=hyper::body::Chunk, Error=Error> + Send {
    let natmap = cx.natmap.clone();
    let args = vec![
        offset.map(|v| OpArg::Offset(v)), 
        length.map(|v| OpArg::Length(v)), 
        buffersize.map(|v| OpArg::BufferSize(v))
        ].into_iter().flatten().collect();
    futures::future::result(cx.uri(path, Op::OPEN, args))
        .map(|uri| HttpxClient::new_get_binary(uri, natmap))
        .flatten_stream()
}

pub fn dir(cx: &HdfsContext, path: &str) -> Result<ListStatusResponse> {
    cx.rt.borrow_mut().block_on(with_timeout(dir_async(cx, path), cx.default_timeout_s))
}

pub fn stat(cx: &HdfsContext, path: &str) -> Result<FileStatusResponse> {
    cx.rt.borrow_mut().block_on(with_timeout(stat_async(cx, path), cx.default_timeout_s))
}

//----------------------------------------------------------------------------------------------------
use std::io::{Read, Write, Seek, SeekFrom, Result as IoResult, Error as IoError, ErrorKind as IoErrorKind};
use std::convert::TryInto;
use futures::Stream;

/// HDFS file read object.
/// Note about position and offset types: we assume that all hdfs/webhdfs lengths and offsets are actually signed 64-bit integers, 
/// according to protocol specifications and JVM specifics (no unsigned)
pub struct ReadHdfsFile {
    cx: HdfsContext,
    path: String,
    len: i64,
    pos: i64
}

impl ReadHdfsFile {
    pub fn open(cx: HdfsContext, path: String) -> Result<ReadHdfsFile> {
        let stat = stat(&cx, &path)?;
        Ok(Self::new(cx, path, stat.file_status.length, 0))
    }
    fn new(cx: HdfsContext, path: String, len: i64, pos: i64) -> Self {
        Self { cx, path, len, pos }
    }
    pub fn len(&self) -> u64 { self.len as u64 }
    pub fn into_context(self) -> HdfsContext { self.cx }
}

impl Read for ReadHdfsFile {
    fn read(&mut self, buf: &mut [u8]) -> IoResult<usize> {

        let buf_len: i64 = buf.len().try_into().map_err(|_| IoError::new(IoErrorKind::InvalidInput, "buffer too big"))?;
        let mut s = file_read_async(&self.cx, &self.path, Some(self.pos), Some(buf_len), None);
        let mut pos: usize = 0;
        
        loop {
            let f = with_timeout(s.into_future().map_err(|(e, _s)| e), self.cx.default_timeout_s);
            match self.cx.rt.borrow_mut().block_on(f) {
                Ok((Some(chunk), s1)) => {
                    s = s1;
                    self.pos += chunk.len() as i64;
                    let bcount = (&mut buf[pos..]).write(&chunk)?;
                    pos += bcount;
                }
                Ok((None, _)) => {
                    break Ok(pos)
                }
                Err(err) => {
                    break Err(err.into())
                }
            }
        }
    }
}

impl Seek for ReadHdfsFile {
    fn seek(&mut self, pos: SeekFrom) -> IoResult<u64> {
        //1. A seek beyond the end of a stream is allowed, but behavior is defined by the implementation --
        //below it either leaves pos unchanged, or seeks to the EOF, depending on which SeekPos is used

        fn offset(pos: i64, offset: i64, len: i64) -> IoResult<i64> {
            match pos.checked_add(offset) {
                Some(p) if p < 0 => Err(IoError::new(IoErrorKind::InvalidInput, "attempt to seek before start")),
                Some(p) if p <= len => Ok(p),
                _ => Ok(pos)
            }
        }

        self.pos = match pos {
            SeekFrom::Current(0) => Ok(self.pos),
            SeekFrom::Current(o) => offset(self.pos, o, self.len),
            SeekFrom::Start(0) => Ok(0),
            SeekFrom::Start(o) => offset(0, o.try_into().map_err(|_| IoError::new(IoErrorKind::InvalidInput, "offset too big"))?, self.len),
            SeekFrom::End(0) => Ok(self.len),
            SeekFrom::End(o) => offset(self.len, o, self.len),                
        }?;
        Ok(self.pos as u64)
    }
}
