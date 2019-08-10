//! Synchronous WebHDFS client
//! 
//! The main client is `SyncHdfsClient`. It is neither `Send` nor `Sync`, so a separate instance must be created in 
//! each thread accessing the API.

use std::io::{Read, Write, Seek, SeekFrom, Result as IoResult, Error as IoError, ErrorKind as IoErrorKind};
use std::convert::TryInto;
use std::time::Duration;
use std::cell::RefCell;
use std::rc::Rc;
use http::Uri;
use tokio::runtime::current_thread::Runtime;
use futures::{Future, Stream};
use crate::error::*;
use crate::datatypes::*;
use crate::async_client::*;
use crate::natmap::NatMap;

pub use crate::op::*;

/// HDFS Connection data, etc.
#[derive(Clone)]
pub struct SyncHdfsClient {
    acx: Rc<HdfsClient>, 
    rt: Rc<RefCell<Runtime>>,
}

pub struct SyncHdfsClientBuilder {
    a: HdfsClientBuilder
}

impl SyncHdfsClientBuilder {
    pub fn new(entrypoint: Uri) -> Self { 
        Self { a: HdfsClientBuilder::new(entrypoint) } 
    }
    pub fn natmap(self, natmap: NatMap) -> Self {
        Self { a: self.a.natmap(natmap), ..self }
    }
    pub fn default_timeout(self, timeout: Duration) -> Self {
        Self { a: self.a.default_timeout(timeout), ..self }
    }
    pub fn build(self) -> Result<SyncHdfsClient> {
         Ok(SyncHdfsClient { 
            acx: Rc::new(self.a.build()), 
            rt: Rc::new(RefCell::new(Runtime::new()?))
        })
    }
}

impl SyncHdfsClient {
    fn from_async_context(acx: HdfsClient)-> Result<Self> {
        Ok(Self { 
            acx: Rc::new(acx), 
            rt: Rc::new(RefCell::new(Runtime::new()?))
        })
    }
    
    #[inline]
    fn exec<R>(&self, f: impl Future<Item=R, Error=Error>) -> Result<R> {
        fn with_timeout<R>(f: impl Future<Item=R, Error=Error>, timeout: Duration) -> impl Future<Item=R, Error=Error> {
            use tokio::prelude::FutureExt;
            f.timeout(timeout).map_err(|e| match e.into_inner() {
                Some(e) => e,
                None => Error::timeout_c("HTTP operation timed out")
            })
        }

        self.rt.borrow_mut().block_on(with_timeout(f, self.acx.default_timeout().clone()))
    }

    pub fn dir(&self, path: &str) -> Result<ListStatusResponse> {
        self.exec(self.acx.dir(path))
    }

    pub fn stat(&self, path: &str) -> Result<FileStatusResponse> {
        self.exec(self.acx.stat(path))
    }
}


/// HDFS file read object.
/// 
/// Note about position and offset types: we assume that all hdfs/webhdfs lengths and offsets are actually signed 64-bit integers, 
/// according to protocol specifications and JVM specifics (no unsigned).
pub struct ReadHdfsFile {
    cx: SyncHdfsClient,
    path: String,
    len: i64,
    pos: i64
}

impl ReadHdfsFile {
    /// Opens the file specified by `path` for reading
    pub fn open(cx: SyncHdfsClient, path: String) -> Result<ReadHdfsFile> {
        let stat = cx.stat(&&path)?;
        Ok(Self::new(cx, path, stat.file_status.length, 0))
    }
    fn new(cx: SyncHdfsClient, path: String, len: i64, pos: i64) -> Self {
        Self { cx, path, len, pos }
    }
    /// File length in bytes
    pub fn len(&self) -> u64 { self.len as u64 }

    /// Splits self into `(sync_client, path, (pos, len))`
    pub fn into_parts(self) -> (SyncHdfsClient, String, (i64, i64)) { (self.cx, self.path, (self.pos, self.len)) }
}

impl Read for ReadHdfsFile {
    fn read(&mut self, buf: &mut [u8]) -> IoResult<usize> {

        let buf_len: i64 = buf.len().try_into().map_err(|_| IoError::new(IoErrorKind::InvalidInput, "buffer too big"))?;
        let mut s = self.cx.acx.open(&self.path, OpenOptions::new().offset(self.pos).length(buf_len));
        let mut pos: usize = 0;
        
        loop {
            match self.cx.exec(s.into_future().map_err(|(e, _s)| e)) {
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


/// HDFS file write object
pub struct WriteHdfsFile {
    cx: SyncHdfsClient,
    path: String,
    opts: AppendOptions
}

impl WriteHdfsFile {
    pub fn create(cx: SyncHdfsClient, path: String, c_opts: CreateOptions, a_opts: AppendOptions) -> Result<WriteHdfsFile> {
        cx.exec(cx.acx.create(&path, vec![], c_opts))?;
        Ok(Self { cx, path, opts: a_opts })
    }
    pub fn append(cx: SyncHdfsClient, path: String, opts: AppendOptions) -> Result<WriteHdfsFile> {
        Ok(Self { cx, path, opts })
    }
}

impl Write for WriteHdfsFile {
    fn write(&mut self, buf: &[u8]) -> IoResult<usize> {
        //TODO We ideally need zero-copy.
        //As tokio doesn't use scoped threading, need kinda unsafe trick to make it think the buf is 'static
        let b = buf.to_owned();
        let f = self.cx.acx.append(&self.path, b, self.opts.clone());
        let _ = self.cx.exec(f)?;
        Ok(buf.len())
    }
    fn flush(&mut self) -> IoResult<()> {
        Ok(())
    }
}