//! Asynchronous WebHDFS client implementation

use std::{future::Future, time::Duration};

use http::{Uri, uri::Parts as UriParts, Method};
use futures::Stream;
use bytes::Bytes;

use crate::uri_tools::*;
use crate::natmap::{NatMap, NatMapPtr};

use crate::error::*;
use crate::rest_client::{HttpyClient, Data};
use crate::datatypes::*;
use crate::op::*;
use crate::config::*;
use crate::future_tools;



//--------------------------------------------------------
/// Asynchronous WebHDFS client
pub struct HdfsClient {
    entrypoint: UriParts,
    natmap: NatMapPtr,
    default_timeout: Duration,
    user_name: Option<String>,
    doas: Option<String>,
    dt: Option<String>
}

/// Builder for `HdfsClient`
pub struct HdfsClientBuilder {
    c: HdfsClient
}

impl HdfsClientBuilder {
    const DEFAULT_TIMEOUT_S: u64 = 30;
    /// Creates new builder from entrypoint
    pub fn new(entrypoint: Uri) -> Self { 
        Self { c: HdfsClient {
                entrypoint: entrypoint.into_parts(),
                natmap: NatMapPtr::empty(),
                default_timeout: Duration::from_secs(Self::DEFAULT_TIMEOUT_S),
                user_name: None,
                doas: None,
                dt: None
        }  } 
    }

    /// Creates new builder from the specified configuration
    pub fn from_explicit_config(conf: Config) -> Self {
        let natmap = conf.natmap.map(
            |natmap| NatMapPtr::new(NatMap::new(natmap.into_iter()).expect("cannot build natmap"))
        ).unwrap_or_else(|| NatMapPtr::empty());
        Self { c: HdfsClient {
                entrypoint: 
                    conf.entrypoint.into_uri().into_parts(),
                natmap: 
                    natmap,
                default_timeout: 
                    conf.default_timeout.unwrap_or_else(|| Duration::from_secs(Self::DEFAULT_TIMEOUT_S)),
                user_name: 
                    conf.user_name,
                doas:
                    conf.doas,
                dt: 
                    conf.dt
        }  } 
    }

    /// Creates new builder, filled with the configuration read from configuration files.
    /// See comments at `crate::config` for detailed semantics.
    pub fn from_config() -> Self { Self::from_explicit_config(read_config()) }

    /// Creates new builder, filled with the configuration read from configuration files, 
    /// if those have been found. Returns `None` otherwise. Note that invalid configuration files
    /// casuse panic rather than returning `None`.
    pub fn from_config_opt() -> Option<Self> { read_config_opt().map(Self::from_explicit_config) }

    pub fn natmap(self, natmap: NatMap) -> Self {
        Self { c: HdfsClient { natmap: NatMapPtr::new(natmap), ..self.c } }
    }
    pub fn default_timeout(self, timeout: Duration) -> Self {
        Self { c: HdfsClient { default_timeout: timeout, ..self.c } }
    }
    pub fn user_name(self, user_name: String) -> Self {
        Self { c: HdfsClient { user_name: Some(user_name), ..self.c } }
    }    
    pub fn doas(self, doas: String) -> Self {
        Self { c: HdfsClient { doas: Some(doas), ..self.c } }
    }
    pub fn delegation_token(self, dt: String) -> Self {
        Self { c: HdfsClient { dt: Some(dt), ..self.c } }
    }
    pub fn build(self) -> HdfsClient { self.c }
}

impl HdfsClient {
    const SVC_MOUNT_POINT: &'static str = "/webhdfs/v1";

    fn natmap(&self) -> NatMapPtr { self.natmap.clone() }
    
    fn uri(&self, file_path: &str, op: Op, args: Vec<OpArg>) -> Result<Uri> {
        let mut b = Uri::builder();        
        if let Some(scheme) = &self.entrypoint.scheme { b = b.scheme(scheme.clone()); }
        if let Some(authority) = &self.entrypoint.authority { b = b.authority(authority.clone()); }

        let q = PathEncoder::new(Self::SVC_MOUNT_POINT).extend(file_path).query();
        let q = if let Some(user) = &self.user_name { q.add_pv("user.name", user) } else { q };
        let q = if let Some(doas) = &self.doas { q.add_pv("doas", doas) } else { q };
        let q = if let Some(dt) = &self.dt { q.add_pv("delegation", dt) } else { q };
        let q = q.add_pv("op", op.op_string());
        let q = args.iter().fold(q, |q, s| s.add_to_url(q));
        let p = q.result();

        b.path_and_query(&p as &[u8]).build()
        .aerr_f(|| format!("Could not build URI: file_path={}, op={:?}, args={:?}", file_path, op, args))
    }

    #[inline]
    fn httpc(&self, path: &str, op: Op, args: Vec<OpArg>) -> Result<HttpyClient> {
        let natmap = self.natmap();
        let uri = self.uri(path, op, args)?;
        Ok(HttpyClient::new(uri, natmap))
    }

    #[inline]
    async fn get_json<T: serde::de::DeserializeOwned + Send + 'static>(&self, path: &str, op: Op, args: Vec<OpArg>) 
    -> Result<T> {
        self.httpc(path, op, args)?.get_json().await
    }

    //NOTE Does not convert to async fn due to 'static inside Data, E0700 hidden type for impl Trait captures... 
    //Presumably a bug in rustc
    #[inline]
    fn data_op(&self, method: Method, path: &str, op: Op, args: Vec<OpArg>, data: Data) 
    -> impl Future<Output=Result<()>> {
        future_tools::simplify_future_result(self.httpc(path, op, args).map(|h| h.post_binary(method, data)))
    }

    #[inline]
    async fn data_op_b(&self, method: Method, path: &str, op: Op, args: Vec<OpArg>) 
    -> Result<bool> {
        self.httpc(path, op, args)?.op_json(method).await.map(|b: Boolean| b.boolean)
    }

    #[inline]
    async fn data_op_e(&self, method: Method, path: &str, op: Op, args: Vec<OpArg>) 
    -> Result<()> {
        self.httpc(path, op, args)?.op_empty(method).await
    }

    pub(crate) fn default_timeout(&self) -> &Duration { &self.default_timeout }

    /// Get directory listing
    pub async fn dir(&self, path: &str) -> Result<ListStatusResponse> {
        self.get_json(path, Op::LISTSTATUS, vec![]).await
    }

    /// Get status
    pub async fn stat(&self, path: &str) -> Result<FileStatusResponse> {
        self.get_json(path, Op::GETFILESTATUS, vec![]).await
    }

    /// Read file data
    pub fn open(&self, path: &str, opts: OpenOptions) -> impl Stream<Item=Result<Bytes>> {
        let binary_stream_result = self.httpc(path, Op::OPEN, opts.into()).map(|h| h.get_binary());
        future_tools::simplify_stream_result(binary_stream_result)
    }

    /// Create a HDFS file and write some data
    pub fn create(&self, path: &str, data: Data, opts: CreateOptions) -> impl Future<Output=Result<()>> {
        //curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=CREATE
        //           [&overwrite=<true |false>][&blocksize=<LONG>][&replication=<SHORT>]
        //           [&permission=<OCTAL>][&buffersize=<INT>]"
        self.data_op(Method::PUT, path, Op::CREATE, opts.into(), data)
    }

    /// Append to a HDFS file
    pub fn append(&self, path: &str, data: Data, opts: AppendOptions) -> impl Future<Output=Result<()>> {
        //curl -i -X POST "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=APPEND[&buffersize=<INT>]"
        self.data_op(Method::POST, path, Op::APPEND, opts.into(), data)
    }

    /// Concatenate files
    pub async fn concat(&self, path: &str, paths: Vec<String>) -> Result<()> {
        //curl -i -X POST "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=CONCAT&sources=<PATHS>"
        self.data_op_e(Method::POST, path, Op::CONCAT, vec![OpArg::Sources(paths)]).await
    }

    /// Make a directory
    pub async fn mkdirs(&self, path: &str, opts: MkdirsOptions) -> Result<bool> {
        //curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=MKDIRS[&permission=<OCTAL>]"
        self.data_op_b(Method::PUT, path, Op::MKDIRS, opts.into()).await
    }

    /// Rename a file/directory
    pub async fn rename(&self, path: &str, destination: String) -> Result<bool> {
        //curl -i -X PUT "<HOST>:<PORT>/webhdfs/v1/<PATH>?op=RENAME&destination=<PATH>"
        self.data_op_b(Method::PUT, path, Op::RENAME, vec![OpArg::Destination(destination)]).await
    }

    /// Create a Symbolic Link
    pub async fn create_symlink(&self, path: &str, destination: String, opts: CreateSymlinkOptions) -> Result<()> {
        //curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=CREATESYMLINK
        //                      &destination=<PATH>[&createParent=<true|false>]"
        let mut o = vec![OpArg::Destination(destination)];
        o.append(&mut opts.into());
        self.data_op_e(Method::PUT, path, Op::CREATESYMLINK, o).await
    }

    /// Delete a File/Directory
    pub async fn delete(&self, path: &str, opts: DeleteOptions) -> Result<bool> {
        //curl -i -X DELETE "http://<host>:<port>/webhdfs/v1/<path>?op=DELETE
        //                      [&recursive=<true|false>]"
        self.data_op_b(Method::DELETE, path, Op::DELETE, opts.into()).await
    }
}
