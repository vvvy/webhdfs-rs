use std::time::Duration;

use http::{Uri, uri::Parts as UriParts};
use futures::{Future, Stream};

use crate::uri_tools::*;
use crate::natmap::{NatMap, NatMapPtr};

use crate::error::*;
use crate::rest_client::HttpxClient;
use crate::datatypes::*;


#[derive(Debug)]
enum Op {
    LISTSTATUS,
    GETFILESTATUS,
    OPEN,
    CREATE,
    APPEND
}

impl Op {
    pub fn op_string(&self) -> &'static str {
        match self {
            Op::LISTSTATUS => "LISTSTATUS",
            Op::GETFILESTATUS => "GETFILESTATUS",
            Op::OPEN => "OPEN",
            Op::CREATE => "CREATE",
            Op::APPEND => "APPEND"
        }
    }
}

#[derive(Debug)]
enum OpArg {
    Offset(i64),
    Length(i64),
    /// `[&buffersize=<INT>]`
    BufferSize(i32),
    /// `[&overwrite=<true |false>]`
    Overwrite(bool),
    /// `[&blocksize=<LONG>]`
    Blocksize(i64),
    /// `[&replication=<SHORT>]`
    Replication(i16),
    /// `[&permission=<OCTAL>]`
    Permission(u16)
}

impl OpArg {
    fn add_to_url(&self, qe: QueryEncoder) -> QueryEncoder {
        match self {
            OpArg::Offset(v) => qe.add_pi("offset", *v),
            OpArg::Length(v) => qe.add_pi("length", *v),
            OpArg::BufferSize(v) => qe.add_pi("buffersize", *v as i64),
            OpArg::Overwrite(v) => qe.add_pb("overwrite", *v),
            OpArg::Blocksize(v) => qe.add_pi("blocksize", *v),
            OpArg::Replication(v) => qe.add_pi("replication", *v as i64),
            OpArg::Permission(v) => qe.add_po("permission", *v),
        }
    }
}

/// Asynchronous WebHDFS client
pub struct HdfsClient {
    entrypoint: UriParts,
    natmap: NatMapPtr,
    default_timeout: Duration
}

impl HdfsClient {
    const SVC_MOUNT_POINT: &'static str = "/webhdfs/v1";
    const DEFAULT_TIMEOUT_S: u64 = 30;

    pub fn new(entrypoint: Uri, natmap: NatMap) -> Self {
        Self { 
            entrypoint: entrypoint.into_parts(), 
            natmap: NatMapPtr::new(natmap), 
            default_timeout: Duration::from_secs(Self::DEFAULT_TIMEOUT_S)
        }
    }
    pub fn from_entrypoint(entrypoint: Uri) -> Self {
        Self { 
            entrypoint: entrypoint.into_parts(), 
            natmap: NatMapPtr::empty(), 
            default_timeout: Duration::from_secs(Self::DEFAULT_TIMEOUT_S)
        }
    }
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

    pub fn dir(&self, path: &str) -> impl Future<Item=ListStatusResponse, Error=Error> + Send {
        let natmap = self.natmap();
        self.uri_result(path, Op::LISTSTATUS, vec![])
            .and_then(|uri| HttpxClient::new_get_json(uri, natmap))
    }

    pub fn stat(&self, path: &str) -> impl Future<Item=FileStatusResponse, Error=Error> + Send {
        let natmap = self.natmap();
        self.uri_result(path, Op::GETFILESTATUS, vec![])
            .and_then(|uri| HttpxClient::new_get_json(uri, natmap))
    }

    pub fn file_read(&self, path: &str, offset: Option<i64>, length: Option<i64>, buffersize: Option<i32>) 
    -> impl Stream<Item=hyper::body::Chunk, Error=Error> + Send {
        let natmap = self.natmap();
        let args = vec![
            offset.map(|v| OpArg::Offset(v)), 
            length.map(|v| OpArg::Length(v)), 
            buffersize.map(|v| OpArg::BufferSize(v))
            ].into_iter().flatten().collect();
        self.uri_result(path, Op::OPEN, args)
            .map(|uri| HttpxClient::new_get_binary(uri, natmap))
            .flatten_stream()
    }

    /// Create a HDFS file and write some data
    pub fn file_create(&self, path: &str, 
        data: Vec<u8>, 
        overwrite: Option<bool>,
        blocksize: Option<i64>,
        replication: Option<i16>,
        permission: Option<u16>, 
        buffersize: Option<i32>) 
    -> impl Future<Item=(), Error=Error> + Send {
        //curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=CREATE
        //           [&overwrite=<true |false>][&blocksize=<LONG>][&replication=<SHORT>]
        //           [&permission=<OCTAL>][&buffersize=<INT>]"
        let natmap = self.natmap();
        let args = vec![
            overwrite.map(|v| OpArg::Overwrite(v)),
            blocksize.map(|v| OpArg::Blocksize(v)), 
            replication.map(|v| OpArg::Replication(v)), 
            permission.map(|v| OpArg::Permission(v)), 
            buffersize.map(|v| OpArg::BufferSize(v))
            ].into_iter().flatten().collect();
        self.uri_result(path, Op::CREATE, args)
            .and_then(|uri| HttpxClient::new_post_binary(uri, natmap, data))
    }

    /// Create a HDFS file and write some data
    pub fn file_append(&self, path: &str, data: Vec<u8>, buffersize: Option<i32>) 
    -> impl Future<Item=(), Error=Error> + Send {
        //curl -i -X POST "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=APPEND[&buffersize=<INT>]"
        let natmap = self.natmap();
        let args = vec![
            buffersize.map(|v| OpArg::BufferSize(v))
            ].into_iter().flatten().collect();
        self.uri_result(path, Op::APPEND, args)
            .and_then(|uri| HttpxClient::new_post_binary(uri, natmap, data))
    }
}
