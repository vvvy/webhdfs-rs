//! Asynchronous WebHDFS client implementation

use std::time::Duration;

use http::{Uri, uri::Parts as UriParts};
use futures::{Future, Stream};

use crate::uri_tools::*;
use crate::natmap::{NatMap, NatMapPtr};

use crate::error::*;
use crate::rest_client::HttpxClient;
use crate::datatypes::*;
use crate::op::*;


//--------------------------------------------------------
/// Asynchronous WebHDFS client
pub struct HdfsClient {
    entrypoint: UriParts,
    natmap: NatMapPtr,
    default_timeout: Duration
}

/// Builder for `HdfsClient`
pub struct HdfsClientBuilder {
    c: HdfsClient
}

impl HdfsClientBuilder {
    const DEFAULT_TIMEOUT_S: u64 = 30;
    pub fn new(entrypoint: Uri) -> Self { 
        Self { c: HdfsClient {
                entrypoint: entrypoint.into_parts(),
                natmap: NatMapPtr::empty(),
                default_timeout: Duration::from_secs(Self::DEFAULT_TIMEOUT_S)
        }  } 
    }
    pub fn natmap(self, natmap: NatMap) -> Self {
        Self { c: HdfsClient { natmap: NatMapPtr::new(natmap), ..self.c } }
    }
    pub fn default_timeout(self, timeout: Duration) -> Self {
        Self { c: HdfsClient { default_timeout: timeout, ..self.c } }
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

        let q0 = PathEncoder::new(Self::SVC_MOUNT_POINT).extend(file_path).query();
        let q1 = q0.add_pv("op", op.op_string());
        let q2 = args.iter().fold(q1, |q, s| s.add_to_url(q));
        let p = q2.result();

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
            .and_then(|uri| HttpxClient::new_get_json(uri, natmap))
    }

    /// Get status
    pub fn stat(&self, path: &str) -> impl Future<Item=FileStatusResponse, Error=Error> + Send {
        let natmap = self.natmap();
        self.uri_result(path, Op::GETFILESTATUS, vec![])
            .and_then(|uri| HttpxClient::new_get_json(uri, natmap))
    }

    /// Read file data
    pub fn open(&self, path: &str, opts: OpenOptions) 
    -> impl Stream<Item=hyper::body::Chunk, Error=Error> + Send {
        let natmap = self.natmap();
        self.uri_result(path, Op::OPEN, opts.into())
            .map(|uri| HttpxClient::new_get_binary(uri, natmap))
            .flatten_stream()
    }

    /// Create a HDFS file and write some data
    pub fn create(&self, path: &str, data: Vec<u8>, opts: CreateOptions) 
    -> impl Future<Item=(), Error=Error> + Send {
        //curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=CREATE
        //           [&overwrite=<true |false>][&blocksize=<LONG>][&replication=<SHORT>]
        //           [&permission=<OCTAL>][&buffersize=<INT>]"
        let natmap = self.natmap();
        self.uri_result(path, Op::CREATE, opts.into())
            .and_then(|uri| HttpxClient::new_post_binary(uri, natmap, data))
    }

    /// Append to a HDFS file
    pub fn append(&self, path: &str, data: Vec<u8>, opts: AppendOptions) 
    -> impl Future<Item=(), Error=Error> + Send {
        //curl -i -X POST "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=APPEND[&buffersize=<INT>]"
        let natmap = self.natmap();
        self.uri_result(path, Op::APPEND, opts.into())
            .and_then(|uri| HttpxClient::new_post_binary(uri, natmap, data))
    }
}
