//! Asynchronous WebHDFS client implementation

use std::time::Duration;
use http::{Uri, uri::Parts as UriParts, Method};
use futures::Stream;
use bytes::Bytes;
use crate::uri_tools::*;
use crate::natmap::{NatMap, NatMapPtr};
use crate::error::*;
use crate::rest_client::{HttpyClient};
pub use crate::rest_client::{ErrorD, DResult, Data};
use crate::datatypes::*;
use crate::op::*;
use crate::config::*;

/// Asynchronous WebHDFS client
pub struct HdfsClient {
    entrypoint: UriParts,
    alt_entrypoint: Option<UriParts>,
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
                alt_entrypoint: None,
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
                alt_entrypoint: 
                    conf.alt_entrypoint.map(|u| u.into_uri().into_parts()),
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

    pub fn alt_entrypoint(self, alt_entrypoint: Uri) -> Self {
        Self { c: HdfsClient { alt_entrypoint: Some(alt_entrypoint.into_parts()), ..self.c } }
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


enum FOAction<T,D> {
    Proceed(Result<T>),
    FailOver(D)
}

/// Failover state. PRIMARY === entrypoint is active. ALT === alt_entrypoint is active
#[derive(Clone, Copy)]
pub enum FOState { PRIMARY, ALT }

impl FOState {
    #[inline]
    pub fn is_alt(&self) -> bool{ if let Self::ALT = self { true } else { false } }
    fn next(self) -> Self { if let Self::ALT = self { Self::PRIMARY } else { Self::ALT } }
}

pub type FOStdResult<T,E> = StdResult<(T, FOState), (E, FOState)>;
pub type FOResult<T> = FOStdResult<T,Error>;
pub type FODResult<T> = FOStdResult<T,ErrorD>;

pub struct FOR;

impl FOR {
    #[inline]
    pub fn split<T,E>(r: FOStdResult<T,E>) -> (StdResult<T,E>, FOState) {
        match r {
            Ok((r, s)) => (Ok(r), s),
            Err((e, s)) => (Err(e), s)
        }
    }
    
    #[inline]
    fn bind<T,E>(r: StdResult<T, E>, fostate: FOState) -> FOStdResult<T,E> {
        match r {
            Ok(r) => Ok((r, fostate)),
            Err(e) => Err((e, fostate))
        }
    }
}



macro_rules! with_failover {
    ([$f1:expr, $f2:expr], $s:expr, $fostate:expr, $pq:expr) => { 
        with_failover!([$f1, $f2, |v| v], $s, $fostate, $pq)
    };

    ([$f1:expr, $f2:expr, $cvt:expr], $s:expr, $fostate:expr, $pq:expr) => { {
        let pq = $pq;
        let (r, fostate) = $s.httpc($fostate, &pq)?;
        let r = $cvt($f1(r).await);
        let (r, fostate) = $s.failover_fsm(fostate, r);
        match r {
            FOAction::Proceed(r) => FOR::bind(r, fostate),
            FOAction::FailOver(_) => {
                let (r, fostate) = $s.httpc(fostate, &pq)?;
                let r = $cvt($f2(r).await);
                FOR::bind(r, fostate)
            }
        } }
    };

    ([$f1:expr, $f2:expr, $ecvt1:expr, $ecvt2:expr], $s:expr, $fostate:expr, $pq:expr, $data:expr) => { {
        let pq = $pq;
        let (r, fostate) = $ecvt1($s.httpc($fostate, &pq))?;
        let r = $f1(r, $data).await;
        let (r, fostate) = $s.failover_fsm_d(fostate, r);
        match r {
            FOAction::Proceed(r) => FOR::bind($ecvt2(r), fostate),
            FOAction::FailOver(data) => {
                let (r, fostate) = $ecvt1($s.httpc(fostate, &pq))?;
                let r = $f2(r, data).await;
                FOR::bind(r, fostate)
            }
        } }
    };
}


impl HdfsClient {
    const SVC_MOUNT_POINT: &'static str = "/webhdfs/v1";

    fn natmap(&self) -> NatMapPtr { self.natmap.clone() }

    fn path_and_query(&self, file_path: &str, op: Op, args: Vec<OpArg>) -> Vec<u8> {
        let q = PathEncoder::new(Self::SVC_MOUNT_POINT).extend(file_path).query();
        let q = if let Some(user) = &self.user_name { q.add_pv("user.name", user) } else { q };
        let q = if let Some(doas) = &self.doas { q.add_pv("doas", doas) } else { q };
        let q = if let Some(dt) = &self.dt { q.add_pv("delegation", dt) } else { q };
        let q = q.add_pv("op", op.op_string());
        let q = args.iter().fold(q, |q, s| s.add_to_url(q));
        q.result()
    }
    
    fn uri(&self, fostate: FOState, pq: &[u8]) -> FOResult<Uri> {
        let mut b = Uri::builder();
        
        let ep = if fostate.is_alt() { 
            if let Some(ep) = &self.alt_entrypoint { ep } else { &self.entrypoint }
        } else { 
            &self.entrypoint 
        };
        
        if let Some(scheme) = &ep.scheme { b = b.scheme(scheme.clone()); }
        if let Some(authority) = &ep.authority { b = b.authority(authority.clone()); }

        let r = b
        .path_and_query(pq)
        .build()
        .aerr_f(|| format!("Could not build URI: path_and_query={}", String::from_utf8_lossy(pq)));

        FOR::bind(r, fostate)
    }

    #[inline]
    fn httpc(&self, fostate: FOState, pq: &[u8]) -> FOResult<HttpyClient> {
        let natmap = self.natmap();
        let (uri, fostate) = self.uri(fostate, pq)?;
        Ok((HttpyClient::new(uri, natmap), fostate))
    }

    #[inline]
    fn is_standby_error(error: &Error) -> bool { 
        //Error { msg: None, cause: RemoteException(RemoteException { 
        //    exception: "StandbyException", 
        //    java_class_name: "org.apache.hadoop.ipc.StandbyException", 
        //    message: "Operation category WRITE is not supported in state standby. Visit https://s.apache.org/sbnn-error" }) }', 
        match error.cause() {
            Cause::RemoteException(RemoteException { exception, ..}) if exception == "StandbyException" => true,
            _ => false
        }
    }

    fn failover_fsm<T>(&self, fostate: FOState, result: Result<T>) -> (FOAction<T, ()>, FOState) {
        match result {
            Err(e) if self.alt_entrypoint.is_some() && Self::is_standby_error(&e) => (FOAction::FailOver(()), fostate.next()),
            //TODO: Err(e) => provide more details in 'error' for the situation
            other => (FOAction::Proceed(other), fostate),
        }
    }

    fn failover_fsm_d<T>(&self, fostate: FOState, result: DResult<T>) -> (FOAction<T, Data>, FOState) {
        match result {
            Err(ErrorD { error, data_opt: Some(data) }) if self.alt_entrypoint.is_some() && Self::is_standby_error(&error) => 
                (FOAction::FailOver(data), fostate.next()),
            Err(ErrorD { error, data_opt: _ }) => 
                //TODO: provide more details describing the situation in 'error' 
                (FOAction::Proceed(Err(error)), fostate),
            Ok(v) => 
                (FOAction::Proceed(Ok(v)), fostate),
        }
    }

    async fn get_json<T>(&self, fostate: FOState, path: &str, op: Op, args: Vec<OpArg>) -> FOResult<T>
    where T: serde::de::DeserializeOwned + Send + 'static
    {
        with_failover!(
            [
                |r: HttpyClient| r.get_json(),
                |r: HttpyClient| r.get_json()
            ],
            self,
            fostate,
            self.path_and_query(path, op, args)
        )
    }

   async fn data_op<'t>(&'t self, fostate: FOState, method: Method, path: &'t str, op: Op, args: Vec<OpArg>, data: Data) 
    -> FODResult<()> {

        fn nod((error, fostate): (Error, FOState)) -> (ErrorD, FOState) { (ErrorD { error, data_opt: None }, fostate) }

        with_failover!(
            [
                |r: HttpyClient, data| r.post_binary(method.clone(), data),
                |r: HttpyClient, data| r.post_binary(method, data),
                |r: FOResult<HttpyClient>| r.map_err(nod),
                |r: Result<()>| r.map_err(ErrorD::lift)
            ],
            self,
            fostate,
            self.path_and_query(path, op, args),
            data
        )
    }

    async fn data_op_b(&self, fostate: FOState, method: Method, path: &str, op: Op, args: Vec<OpArg>) 
    -> FOResult<bool> {
        with_failover!(
            [
                |r: HttpyClient| r.op_json(method.clone()),
                |r: HttpyClient| r.op_json(method),
                |r: Result<Boolean>| r.map(|b: Boolean| b.boolean)
            ],
            self,
            fostate,
            self.path_and_query(path, op, args)
            )
    }    

    async fn data_op_e(&self, fostate: FOState, method: Method, path: &str, op: Op, args: Vec<OpArg>) 
    -> FOResult<()> {
        with_failover!(
            [
                |r: HttpyClient| r.op_empty(method.clone()),
                |r: HttpyClient| r.op_empty(method)
            ],
            self,
            fostate,
            self.path_and_query(path, op, args)
            
        )
    }

    /*
    //Needs stable async closures
    async fn generic_request<T>(&self, 
        fostate: FOState, pq: Vec<u8>, 
        f1: impl FnOnce(HttpyClient) -> Result<T>,
        f2: impl FnOnce(HttpyClient) -> Result<T>) -> FOResult<T> {
        let (r, fostate) = self.httpc(fostate, &pq)?;
        let r = f1(r);
        let (r, fostate) = self.failover_fsm(fostate, r);
        match r {
            FOAction::Proceed(r) => with_fostate(r, fostate),
            FOAction::FailOver(_) => {
                let (r, fostate) = self.httpc(fostate, &pq)?;
                let r = f2(r);
                with_fostate(r, fostate)
            }
        }     
    }

    #[inline]
    async fn data_op_e2(&self, fostate: FOState, method: Method, path: &str, op: Op, args: Vec<OpArg>) 
    -> FOResult<()> {
        self.generic_request(fostate, self.path_and_query(path, op, args),
            async |r| r.op_empty(method.clone()).await,
            async |r| r.op_empty(method.clone()).await
        ).await
    }
    */   

    #[inline]
    pub(crate) fn default_timeout(&self) -> &Duration { &self.default_timeout }

    /// Get directory listing
    pub async fn dir(&self, fostate: FOState, path: &str) -> FOResult<ListStatusResponse> {
        self.get_json(fostate, path, Op::LISTSTATUS, vec![]).await
    }

    /// Get status
    pub async fn stat(&self, fostate: FOState, path: &str) -> FOResult<FileStatusResponse> {
        self.get_json(fostate, path, Op::GETFILESTATUS, vec![]).await
    }

    /// Read file data
    pub async fn open(&self, fostate: FOState, path: &str, opts: OpenOptions) -> FOResult<Box<dyn Stream<Item=Result<Bytes>>+Unpin>> {
        with_failover!(
            [
                |r: HttpyClient| r.get_binary(),
                |r: HttpyClient| r.get_binary()
            ],
            self,
            fostate,
            self.path_and_query(path, Op::OPEN, opts.into())
        )
    }

    /// Create a HDFS file and write some data
    pub async fn create<'t>(&'t self, fostate: FOState, path: &'t str, data: Data, opts: CreateOptions) -> FODResult<()> {
        //curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=CREATE
        //           [&overwrite=<true |false>][&blocksize=<LONG>][&replication=<SHORT>]
        //           [&permission=<OCTAL>][&buffersize=<INT>]"
        self.data_op(fostate, Method::PUT, path, Op::CREATE, opts.into(), data).await
    }

    /// Append to a HDFS file
    pub async fn append<'t>(&'t self, fostate: FOState, path: &'t str, data: Data, opts: AppendOptions) -> FODResult<()> {
        //curl -i -X POST "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=APPEND[&buffersize=<INT>]"
        self.data_op(fostate, Method::POST, path, Op::APPEND, opts.into(), data).await
    }

    /// Concatenate files
    pub async fn concat(&self, fostate: FOState, path: &str, paths: Vec<String>) -> FOResult<()> {
        //curl -i -X POST "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=CONCAT&sources=<PATHS>"
        self.data_op_e(fostate, Method::POST, path, Op::CONCAT, vec![OpArg::Sources(paths)]).await
    }

    /// Make a directory
    pub async fn mkdirs(&self, fostate: FOState, path: &str, opts: MkdirsOptions) -> FOResult<bool> {
        //curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=MKDIRS[&permission=<OCTAL>]"
        self.data_op_b(fostate, Method::PUT, path, Op::MKDIRS, opts.into()).await
    }

    /// Rename a file/directory
    pub async fn rename(&self, fostate: FOState, path: &str, destination: String) -> FOResult<bool> {
        //curl -i -X PUT "<HOST>:<PORT>/webhdfs/v1/<PATH>?op=RENAME&destination=<PATH>"
        self.data_op_b(fostate, Method::PUT, path, Op::RENAME, vec![OpArg::Destination(destination)]).await
    }

    /// Create a Symbolic Link
    pub async fn create_symlink(&self, fostate: FOState, path: &str, destination: String, opts: CreateSymlinkOptions) -> FOResult<()> {
        //curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=CREATESYMLINK
        //                      &destination=<PATH>[&createParent=<true|false>]"
        let mut o = vec![OpArg::Destination(destination)];
        o.append(&mut opts.into());
        self.data_op_e(fostate, Method::PUT, path, Op::CREATESYMLINK, o).await
    }

    /// Delete a File/Directory
    pub async fn delete(&self, fostate: FOState, path: &str, opts: DeleteOptions) -> FOResult<bool> {
        //curl -i -X DELETE "http://<host>:<port>/webhdfs/v1/<path>?op=DELETE
        //                      [&recursive=<true|false>]"
        self.data_op_b(fostate, Method::DELETE, path, Op::DELETE, opts.into()).await
    }

}
