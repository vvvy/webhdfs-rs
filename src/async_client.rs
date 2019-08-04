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
enum OpArg {
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

    pub(crate) fn default_timeout(&self) -> &Duration { &self.default_timeout }

    pub fn dir(&self, path: &str) -> impl Future<Item=ListStatusResponse, Error=Error> + Send {
        let natmap = self.natmap();
        futures::future::result(self.uri(path, Op::LISTSTATUS, vec![]))
            .and_then(|uri| HttpxClient::new_get_json(uri, natmap))
    }

    pub fn stat(&self, path: &str) -> impl Future<Item=FileStatusResponse, Error=Error> + Send {
        let natmap = self.natmap();
        futures::future::result(self.uri(path, Op::GETFILESTATUS, vec![]))
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
        futures::future::result(self.uri(path, Op::OPEN, args))
            .map(|uri| HttpxClient::new_get_binary(uri, natmap))
            .flatten_stream()
    }
}
