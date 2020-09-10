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
use tokio::runtime::{Builder, Runtime};
use futures::{Future, Stream, stream::StreamExt};
use bytes::Bytes;
use crate::error::*;
use crate::datatypes::*;
use crate::async_client::*;
use crate::natmap::NatMap;

pub use crate::op::*;

#[inline]
fn single_threaded_runtime() -> Result<Runtime> { Ok(Builder::new().basic_scheduler().enable_io().enable_time().build()?) }

/// HDFS Connection data, etc.
#[derive(Clone)]
pub struct SyncHdfsClient {
    acx: Rc<HdfsClient>, 
    rt: Rc<RefCell<Runtime>>,
    fostate: FOState
}

pub struct SyncHdfsClientBuilder {
    a: HdfsClientBuilder
}

impl SyncHdfsClientBuilder {
    pub fn new(entrypoint: Uri) -> Self { 
        Self { a: HdfsClientBuilder::new(entrypoint) } 
    }
    pub fn from_config() -> Self { 
        Self { a: HdfsClientBuilder::from_config() } 
    }
    pub fn from_config_opt() -> Option<Self> { 
        HdfsClientBuilder::from_config_opt().map(|a| Self { a })
    }
    pub fn natmap(self, natmap: NatMap) -> Self {
        Self { a: self.a.natmap(natmap), ..self }
    }
    pub fn default_timeout(self, timeout: Duration) -> Self {
        Self { a: self.a.default_timeout(timeout), ..self }
    }
    pub fn user_name(self, user_name: String) -> Self {
        Self { a: self.a.user_name(user_name), ..self }
    }
    pub fn doas(self, doas: String) -> Self {
        Self { a: self.a.doas(doas), ..self }
    }
    pub fn delegation_token(self, dt: String) -> Self {
        Self { a: self.a.delegation_token(dt), ..self }
    }
    pub fn build(self) -> Result<SyncHdfsClient> {
         Ok(SyncHdfsClient { 
            acx: Rc::new(self.a.build()), 
            rt: Rc::new(RefCell::new(single_threaded_runtime()?)),
            fostate: FOState::PRIMARY
        })
    }
}

impl SyncHdfsClient {
    pub fn from_async(acx: HdfsClient)-> Result<Self> {
        Ok(Self { 
            acx: Rc::new(acx), 
            rt: Rc::new(RefCell::new(single_threaded_runtime()?)),
            fostate: FOState::PRIMARY
        })
    }
    
    #[inline]
    fn exec<R, E>(&self, f: impl Future<Output=FOStdResult<R, E>>) -> FOStdResult<R, E> where E: From<tokio::time::Elapsed>{
        async fn with_timeout<R, E>(f: impl Future<Output=FOStdResult<R, E>>, fostate: FOState, timeout: Duration) -> FOStdResult<R, E> where E: From<tokio::time::Elapsed> {
            Ok(tokio::time::timeout(timeout, f).await.map_err(|e| (e.into(), fostate))??)
        }
        self.rt.borrow_mut().block_on(with_timeout(f, self.fostate, self.acx.default_timeout().clone()))
    }
    
    #[inline]
    fn exec0<R>(&self, f: impl Future<Output=R>) -> Result<R> {
        async fn with_timeout<R>(f: impl Future<Output=R>, timeout: Duration) -> Result<R> {
            Ok(tokio::time::timeout(timeout, f).await?)
        }
        self.rt.borrow_mut().block_on(with_timeout(f, self.acx.default_timeout().clone()))
    }

    #[inline]
    fn foresult<T, E>(&mut self, r: FOStdResult<T, E>) -> StdResult<T, E> {
        let (r, fostate) = FOR::split(r);
        self.fostate = fostate;
        r
    }

    /// Open a file for reading
    pub fn open(&mut self, path: &str, open_options: OpenOptions) -> Result<Box<dyn Stream<Item=Result<Bytes>>+Unpin>> {
        let fs = self.acx.open(self.fostate, path, open_options);
        let r = self.exec0(fs)?;
        self.foresult(r)
    }

    /// Append to a file
    pub fn append(&mut self, path: &str, data: Data, append_options: AppendOptions) -> DResult<()> {
        let f = self.acx.append(self.fostate, path, data, append_options);
        let r = self.exec(f);
        self.foresult(r)
    }

    /// Create file
    pub fn create(&mut self, path: &str, data: Data, opts: CreateOptions) -> DResult<()> {
        let f = self.acx.create(self.fostate, path, data, opts);
        let r = self.exec(f);
        self.foresult(r)
    }

    fn save_stream<W: Write>(&self, input: impl Stream<Item=Result<Bytes>>, output: &mut W) -> Result<()> {
        fn write_bytes<W: Write>(b: &Bytes, w: &mut W) -> Result<()> {
            if w.write(&b)? != b.len() {
                Err(app_error!(generic "Short write"))
            } else {
                Ok(())
            }
        }
        let mut input = Box::pin(input);
        loop {
            let f = input.into_future();
            let (ob, input2) = self.exec0(f)?;
            match ob {
                Some(Ok(bytes)) => write_bytes(&bytes, output)?,
                Some(Err(e)) => break Err(e),
                None => break Ok(())
            }
            input = input2;
        }
    }

    /// Get a file (read it from hdfs and save to local fs)
    #[inline]
    pub fn get_file<W: Write>(&mut self, input: &str, output: &mut W) -> Result<()> {    
        let s = self.open(input, OpenOptions::new())?;
        self.save_stream(s, output)
    }

    /// Get directory listing
    pub fn dir(&mut self, path: &str) -> Result<ListStatusResponse> {
        let r = self.acx.dir(self.fostate, path);
        let r = self.exec(r);
        self.foresult(r)
    }

    /// Stat a file /dir
    pub fn stat(&mut self, path: &str) -> Result<FileStatusResponse> {
        let r = self.acx.stat(self.fostate, path);
        let r = self.exec(r);
        self.foresult(r)
    }

    /// Concat File(s)
    pub fn concat(&mut self, path: &str, paths: Vec<String>) -> Result<()> {
        let r = self.acx.concat(self.fostate, path, paths);
        let r = self.exec(r);
        self.foresult(r)
    }

    /// Make a Directory
    pub fn mkdirs(&mut self, path: &str, opts: MkdirsOptions) -> Result<bool> {
        let r = self.acx.mkdirs(self.fostate, path, opts);
        let r = self.exec(r);
        self.foresult(r)
    }

    /// Rename a file/directory
    pub fn rename(&mut self, path: &str, destination: String) -> Result<bool> {
        let r = self.acx.rename(self.fostate, path, destination);
        let r = self.exec(r);
        self.foresult(r)
    }

    /// Create a Symbolic Link
    pub fn create_symlink(&mut self, path: &str, destination: String, opts: CreateSymlinkOptions) ->  Result<()> {
        let r = self.acx.create_symlink(self.fostate, path, destination, opts);
        let r = self.exec(r);
        self.foresult(r)
    }

    /// Delete a File/Directory
    pub fn delete(&mut self, path: &str, opts: DeleteOptions) -> Result<bool> {
        let r = self.acx.delete(self.fostate, path, opts);
        let r = self.exec(r);
        self.foresult(r)
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
    pub fn open(mut cx: SyncHdfsClient, path: String) -> Result<ReadHdfsFile> {
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
        let s = self.cx.open(&self.path, OpenOptions::new().offset(self.pos).length(buf_len))?;
        let mut pos: usize = 0;
        
        let mut s = Box::pin(s);
        loop {
            let f = s.into_future();
            match self.cx.exec0(f)? {
                (Some(Ok(chunk)), s1) => {
                    s = s1;
                    self.pos += chunk.len() as i64;
                    let bcount = (&mut buf[pos..]).write(&chunk)?;
                    pos += bcount;
                }
                (Some(Err(e)), _) => {
                    break Err(e.into())
                }
                (None, _) => {
                    break Ok(pos)
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
    pub fn create(mut cx: SyncHdfsClient, path: String, c_opts: CreateOptions, a_opts: AppendOptions) -> Result<WriteHdfsFile> {
        cx.create(&path, crate::rest_client::data_empty(), c_opts).map_err(ErrorD::drop)?;
        Ok(Self { cx, path, opts: a_opts })
    }
    pub fn append(cx: SyncHdfsClient, path: String, opts: AppendOptions) -> Result<WriteHdfsFile> {
        Ok(Self { cx, path, opts })
    }
    /// Splits self into `(sync_client, path, (pos, len))`
    pub fn into_parts(self) -> (SyncHdfsClient, String) { (self.cx, self.path) }

    ///zero-copy write (work around tokio's lack of support for scoped threading)
    #[cfg(feature = "zero-copy-on-write")]
    fn do_write(&mut self, buf: &[u8]) -> DResult<()> {
        let b: & 'static [u8] = unsafe { std::mem::transmute(buf) };
        self.cx.append(&self.path, crate::rest_client::data_borrowed(b), self.opts.clone())
    }

    #[cfg(not(feature = "zero-copy-on-write"))]
    fn do_write(&mut self, buf: &[u8]) -> DResult<()> {
        let b = buf.to_owned();
        self.cx.append(&self.path, crate::rest_client::data_owned(b), self.opts.clone())
    }
}

impl Write for WriteHdfsFile {
    fn write(&mut self, buf: &[u8]) -> IoResult<usize> {
        let () = self.do_write(buf).map_err(ErrorD::drop)?;
        Ok(buf.len())
    }
    fn flush(&mut self) -> IoResult<()> {
        Ok(())
    }
}