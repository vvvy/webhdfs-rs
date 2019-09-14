//! Asynchronous WebHDFS client implementation

use std::time::Duration;

use http::{Uri, uri::Parts as UriParts, Method};
use futures::{Future, Stream};

use crate::uri_tools::*;
use crate::natmap::{NatMap, NatMapPtr};

use crate::error::*;
use crate::rest_client::{HttpyClient, Data};
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

    pub(crate) fn default_timeout(&self) -> &Duration { &self.default_timeout }

    /// Get directory listing
    pub fn dir(&self, path: &str) -> impl Future<Item=ListStatusResponse, Error=Error> + Send {
        let natmap = self.natmap();
        self.uri_result(path, Op::LISTSTATUS, vec![])
            .and_then(|uri| HttpyClient::new(uri, natmap).get_json())
    }

    /// Get status
    pub fn stat(&self, path: &str) -> impl Future<Item=FileStatusResponse, Error=Error> + Send {
        let natmap = self.natmap();
        self.uri_result(path, Op::GETFILESTATUS, vec![])
            .and_then(|uri| HttpyClient::new(uri, natmap).get_json())
    }

    /// Read file data
    pub fn open(&self, path: &str, opts: OpenOptions) 
    -> impl Stream<Item=hyper::body::Chunk, Error=Error> + Send {
        let natmap = self.natmap();
        self.uri_result(path, Op::OPEN, opts.into())
            .map(|uri| HttpyClient::new(uri, natmap).get_binary())
            .flatten_stream()
    }

    /// Create a HDFS file and write some data
    pub fn create(&self, path: &str, data: Data, opts: CreateOptions) 
    -> impl Future<Item=(), Error=Error> + Send {
        //curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=CREATE
        //           [&overwrite=<true |false>][&blocksize=<LONG>][&replication=<SHORT>]
        //           [&permission=<OCTAL>][&buffersize=<INT>]"
        let natmap = self.natmap();
        self.uri_result(path, Op::CREATE, opts.into())
            .and_then(|uri| HttpyClient::new(uri, natmap).post_binary(Method::PUT, data))
    }

    /// Append to a HDFS file
    pub fn append(&self, path: &str, data: Data, opts: AppendOptions) 
    -> impl Future<Item=(), Error=Error> + Send {
        //curl -i -X POST "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=APPEND[&buffersize=<INT>]"
        let natmap = self.natmap();
        self.uri_result(path, Op::APPEND, opts.into())
            .and_then(|uri| HttpyClient::new(uri, natmap).post_binary(Method::POST, data))
    }
}
