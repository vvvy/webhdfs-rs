//! Asynchronous WebHDFS client implementation

use std::time::Duration;

use http::{Uri, uri::Parts as UriParts, Method};
use futures::{Future, Stream};
use bytes::Bytes;

use crate::uri_tools::*;
use crate::natmap::{NatMap, NatMapPtr};

use crate::error::*;
use crate::rest_client::{HttpyClient, Data, data_empty};
use crate::datatypes::*;
use crate::op::*;


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
    /// Creates new builder, filled with the configuration read from the configuration files.
    /// See comments at `crate::config` for detailed semantics.
    pub fn from_config() -> Self {
        let conf = crate::config::read_config();
        Self { c: HdfsClient {
                entrypoint: 
                    conf.entrypoint.into_uri().into_parts(),
                natmap: 
                    NatMapPtr::empty(),
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
        if let Some(scheme) = &self.entrypoint.scheme { b.scheme(scheme.clone()); }
        if let Some(authority) = &self.entrypoint.authority { b.authority(authority.clone()); }

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
    fn uri_result(&self, path: &str, op: Op, args: Vec<OpArg>) -> impl Future<Item=Uri, Error=Error> + Send {
        futures::future::result(self.uri(path, op, args))
    }

    #[inline]
    fn get_json<T: serde::de::DeserializeOwned + Send + 'static>(&self, path: &str, op: Op, args: Vec<OpArg>) 
    -> impl Future<Item=T, Error=Error> + Send {
        let natmap = self.natmap();
        self.uri_result(path, op, args).and_then(|uri| HttpyClient::new(uri, natmap).get_json())
    }

    #[inline]
    fn data_op(&self, method: Method, path: &str, op: Op, args: Vec<OpArg>, data: Data) 
    -> impl Future<Item=(), Error=Error> + Send {
        let natmap = self.natmap();
        self.uri_result(path, op, args).and_then(|uri| HttpyClient::new(uri, natmap).post_binary(method, data))
    }

    pub(crate) fn default_timeout(&self) -> &Duration { &self.default_timeout }

    /// Get directory listing
    pub fn dir(&self, path: &str) -> impl Future<Item=ListStatusResponse, Error=Error> + Send {
        self.get_json(path, Op::LISTSTATUS, vec![])
    }

    /// Get status
    pub fn stat(&self, path: &str) -> impl Future<Item=FileStatusResponse, Error=Error> + Send {
        self.get_json(path, Op::GETFILESTATUS, vec![])
    }

    /// Read file data
    pub fn open(&self, path: &str, opts: OpenOptions) -> impl Stream<Item=Bytes, Error=Error> + Send {
        let natmap = self.natmap();
        self.uri_result(path, Op::OPEN, opts.into())
            .map(|uri| HttpyClient::new(uri, natmap).get_binary())
            .flatten_stream()
    }

    /// Create a HDFS file and write some data
    pub fn create(&self, path: &str, data: Data, opts: CreateOptions) -> impl Future<Item=(), Error=Error> + Send {
        //curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=CREATE
        //           [&overwrite=<true |false>][&blocksize=<LONG>][&replication=<SHORT>]
        //           [&permission=<OCTAL>][&buffersize=<INT>]"
        self.data_op(Method::PUT, path, Op::CREATE, opts.into(), data)
    }

    /// Append to a HDFS file
    pub fn append(&self, path: &str, data: Data, opts: AppendOptions) -> impl Future<Item=(), Error=Error> + Send {
        //curl -i -X POST "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=APPEND[&buffersize=<INT>]"
        self.data_op(Method::POST, path, Op::APPEND, opts.into(), data)
    }

    /// Concatenate files
    pub fn concat(&self, path: &str, paths: Vec<String>) -> impl Future<Item=(), Error=Error> + Send {
        //curl -i -X POST "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=CONCAT&sources=<PATHS>"
        self.data_op(Method::POST, path, Op::CONCAT, vec![OpArg::Sources(paths)], data_empty())
    }

    /// Make a directory
    pub fn mkdirs(&self, path: &str, opts: MkdirsOptions) -> impl Future<Item=(), Error=Error> + Send {
        //curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=MKDIRS[&permission=<OCTAL>]"
        self.data_op(Method::PUT, path, Op::MKDIRS, opts.into(), data_empty())
    }

    /// Rename a file/directory
    pub fn rename(&self, path: &str, destination: String) -> impl Future<Item=(), Error=Error> + Send {
        //curl -i -X PUT "<HOST>:<PORT>/webhdfs/v1/<PATH>?op=RENAME&destination=<PATH>"
        self.data_op(Method::PUT, path, Op::RENAME, vec![OpArg::Destination(destination)], data_empty())
    }

    /// Create a Symbolic Link
    pub fn create_symlink(&self, path: &str, destination: String, opts: CreateSymlinkOptions) -> impl Future<Item=(), Error=Error> + Send {
        //curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=CREATESYMLINK
        //                      &destination=<PATH>[&createParent=<true|false>]"
        let mut o = vec![OpArg::Destination(destination)];
        o.append(&mut opts.into());
        self.data_op(Method::PUT, path, Op::CREATESYMLINK, o, data_empty())
    }

    /// Delete a File/Directory
    pub fn delete(&self, path: &str, opts: DeleteOptions) -> impl Future<Item=(), Error=Error> + Send {
        //curl -i -X DELETE "http://<host>:<port>/webhdfs/v1/<path>?op=DELETE
        //                      [&recursive=<true|false>]"
        self.data_op(Method::DELETE, path, Op::DELETE, opts.into(), data_empty())
    }
}
