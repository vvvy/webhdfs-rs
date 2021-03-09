
use futures::{Stream, FutureExt, StreamExt};
use hyper::{
    Request, Response, Body, Uri,
    client::{Client, ResponseFuture, HttpConnector},
    body::to_bytes
};
use hyper_tls::HttpsConnector;
use http::{uri::Scheme, request::Builder as RequestBuilder, method::Method};
use bytes::{Bytes, Buf};
use mime::Mime;
use log::{debug,trace};
use crate::error::*;
use crate::datatypes::RemoteExceptionResponse;
use crate::natmap::NatMapPtr;
use crate::https::*;

/// Required response content-type
#[derive(Debug, PartialEq, Copy, Clone)]
enum RCT {
    /// Response must not have content-type (i.e., must be empty)
    None,
    /// Response must be application/json, with optional charset=utf-8
    JSON,
    /// Response must be application/octet-stream
    Binary
}

#[inline]
fn redirect_filter(res: Response<Body>) -> Result<Response<Body>> {
    let status = res.status();
    if status.is_redirection() {
        if let Some(location) = res.headers().get(hyper::header::LOCATION) {
            Err(Error::from_http_redirect(status.as_u16(), location.to_str()?.to_string()))
        } else {
            Err(app_error!(generic "Redirect without Location header"))
        }
    } else {
        Ok(res)
    }
}

#[inline]
async fn error_and_ct_filter(ct_required: RCT, res: Response<Body>) -> Result<Response<Body>> {

    #[inline]
    fn content_type_extractor(res: &Response<Body>) -> Result<Option<Mime>> {
        use std::str::FromStr;
        let m = res.headers()
            .get(hyper::header::CONTENT_TYPE)
            .map(|s| s.to_str().map(|x| mime::Mime::from_str(x)));
        match m {
            Some(Ok(Ok(ct))) => Ok(Some(ct)),
            Some(Ok(Err(ect))) => Err(ect.into()),
            Some(Err(ect)) => Err(ect.into()),
            None => Ok(None) //Err(app_error!(generic "no content type found (application/json or application/octet-stream required)"))
        }
    }

    #[inline]
    fn match_mimes(ct: &Option<Mime>, ct_required: RCT) -> bool {
        match (ct, ct_required) {
            (Some(ct), RCT::JSON) => match (ct.type_(), ct.subtype(), ct.get_param("charset")) {
                (mime::APPLICATION, mime::JSON, Some(mime::UTF_8)) => true,
                (mime::APPLICATION, mime::JSON, None) => true,
                _ => false
            }
            (Some(ct), RCT::Binary) => mime::APPLICATION_OCTET_STREAM.eq(ct),
            (None, RCT::None) => true,
            _ => false
        }
    }

    let ct = content_type_extractor(&res)?;
    let status = res.status();
    if status.is_success() {
        if match_mimes(&ct, ct_required) {
            Ok(res)
        } else {
            Err(app_error!(generic "Invald content type: required='{:?}' found='{:?}'", ct_required, ct))
        }
    } else {
        //Failure: try to retrieve JSON error message
        if match_mimes(&ct, RCT::JSON) {
            match to_bytes(res.into_body()).await {
                Ok(buf) => match serde_json::from_reader::<_, RemoteExceptionResponse>(buf.clone().reader()) {
                    Ok(rer) => Err(rer.remote_exception.into()),
                    Err(e) => Err(app_error!(generic "JSON-error deseriaization error: {}, recovered text: '{}'", 
                        e, String::from_utf8_lossy(buf.chunk().as_ref())
                    ))
                }
                Err(e) => Err(app_error!(generic "JSON-error aggregation error: {}", e))
            }
        } else {
            debug!("Remote error w/o JSON content: {:?}", res);
            Err(app_error!(generic "Remote error: {}, content-type: {:?}", status, ct))
        }
    }
}

#[inline]
async fn extract_json<R>(res: Response<Body>) -> Result<R>
where R: serde::de::DeserializeOwned + Send { 
    trace!("HTTP JSON Response {} ct={:?} cl={:?}", 
        res.status(), res.headers().get(hyper::header::CONTENT_TYPE), res.headers().get(hyper::header::CONTENT_LENGTH)
    );
    let buf = to_bytes(res.into_body()).await?;
    serde_json::from_reader(buf.reader()).aerr("JSON deseriaization error")
}

#[inline]
async fn extract_binary(res: Response<Body>) -> impl Stream<Item=Result<Bytes>> + Unpin {
    trace!("HTTP Binary Response {} ct={:?} cl={:?}", 
        res.status(), 
        res.headers().get(hyper::header::CONTENT_TYPE), 
        res.headers().get(hyper::header::CONTENT_LENGTH)
    );
    res.into_body().map(|r| r.aerr("Binary sream read error"))
}

#[inline]
async fn extract_empty(res: Response<Body>) -> Result<()> {
    trace!("HTTP Empty Response {} ct={:?} cl={:?}", 
        res.status(), 
        res.headers().get(hyper::header::CONTENT_TYPE), 
        res.headers().get(hyper::header::CONTENT_LENGTH)
    );
    let buf = to_bytes(res.into_body()).await?;
    if !buf.has_remaining() {
        Ok(())
    } else {
        Err(app_error!(generic "Unexpected non-empty response received, where empty is expected"))
    }
}

#[inline]
fn http_empty_body(request: RequestBuilder) -> Result<Request<Body>> {
    Ok(request.body(Body::empty())?)
}

/// Data being sent out
pub type Data = std::borrow::Cow<'static, [u8]>;

#[cfg(not(feature = "zero-copy-on-write"))]
#[inline]
pub fn data_owned(d: Vec<u8>) -> Data { std::borrow::Cow::Owned(d) }

#[cfg(feature = "zero-copy-on-write")]
#[inline]
pub fn data_borrowed(d: &'static [u8]) -> Data { std::borrow::Cow::Borrowed(d) }

#[inline]
pub fn data_empty() -> Data { std::borrow::Cow::Borrowed(&[]) }


#[inline]
fn http_binary_body(request: RequestBuilder, payload: Data) -> Result<Request<Body>> {
    Ok(request.body(Body::from(payload))?)
}

/// Error that contains optional data recovered from an unsuccessful write operation
pub struct ErrorD {
    pub error: Error,
    pub data_opt: Option<Data>
}

impl ErrorD {
    #[inline]
    pub fn new(error: Error, data_opt: Option<Data>) -> Self { Self { error, data_opt } }
    #[inline]
    pub fn d(error: Error, data: Data) -> Self { Self::new(error, Some(data)) }
    #[inline]
    pub fn lift(error: Error) -> Self { Self::new(error, None) }
    #[inline]
    pub fn drop(Self { error, data_opt: _ } : Self) -> Error { error }
}

impl From<tokio::time::error::Elapsed> for ErrorD {
    fn from(e: tokio::time::error::Elapsed) -> Self { Self::lift(e.into()) }
}

/// Result with optional data recovered from error
pub type DResult<T> = StdResult<T, ErrorD>;


pub struct HttpxEndpoint {
    uri: Uri,
    https_settings: Option<HttpsSettingsPtr>
}

impl HttpxEndpoint {
    pub fn new(uri: Uri, https_settings: Option<HttpsSettingsPtr>) -> Self { Self { uri, https_settings }  }
    //pub fn uri(&self) -> &Uri { &self.uri }
    pub fn https_settings(&self) -> &Option<HttpsSettingsPtr> { &self.https_settings }
}

/// HTTP(S) client
/// TODO seems like HttpsConnector supports http:// urls as well, check it
enum Httpx {
    Http(Client<HttpConnector, Body>),
    Https(Client<HttpsConnector<HttpConnector>, Body>)
}

impl Httpx {
    fn new(endpoint: &HttpxEndpoint) -> Httpx {
        if Some(&Scheme::HTTPS) == endpoint.uri.scheme() {
            let connector = if let Some(cfg) = &endpoint.https_settings {
                https_connector(cfg)
            } else {
                HttpsConnector::new()
            };
            Httpx::Https(Client::builder().build::<_, hyper::Body>(connector))
        } else {
            Httpx::Http(Client::new())
        }
    }

    fn request_raw(&self, r: Request<Body>) -> ResponseFuture {
        match self {
            Httpx::Http(c) => c.request(r),
            Httpx::Https(c) => c.request(r),
        }
    }
}

struct HttpxClient {
    endpoint: Httpx
}

impl HttpxClient
{
    fn new(endpoint: &HttpxEndpoint) -> Self { Self { endpoint: Httpx::new(endpoint) } }

    #[inline]
    fn create_request(&self, method: Method, uri: Uri) -> RequestBuilder {
        trace!("{} {}", method, uri);
        RequestBuilder::new()
            .method(method)
            .uri(uri)
    }

    #[inline]
    async fn get_like_future(&self, uri: Uri, method: Method) -> Result<Response<Body>> {
        let builder = self.create_request(method, uri);
        let body = http_empty_body(builder)?;
        let request = self.endpoint.request_raw(body);
        let response = request.await?;
        Ok(response)
    }

    #[inline]
    async fn post_like_future(&self, uri: Uri, method: Method, payload: Data) -> Result<Response<Body>> {
        let builder = self.create_request(method, uri);
        let body = http_binary_body(builder, payload)?;
        let request = self.endpoint.request_raw(body);
        let response = request.await?;
        Ok(response)
    }

    async fn new_get_like(endpoint: HttpxEndpoint, method: Method) -> Result<Response<Body>> {
        Self::new(&endpoint).get_like_future(endpoint.uri, method).await
    }

    async fn new_post_like(endpoint: HttpxEndpoint, method: Method, payload: Data) -> Result<Response<Body>> {
        Self::new(&endpoint).post_like_future(endpoint.uri, method, payload).await
    }
}

pub struct HttpyClient {
    endpoint: HttpxEndpoint, 
    natmap: NatMapPtr
}

impl HttpyClient {
    pub fn new(endpoint: HttpxEndpoint, natmap: NatMapPtr) -> Self { Self { endpoint, natmap } }

    #[inline]
    async fn redirect_uri(endpoint: HttpxEndpoint, method: Method, natmap: NatMapPtr)-> Result<HttpxEndpoint> {
        let https_settings = endpoint.https_settings().clone();
        let r = HttpxClient::new_get_like(endpoint, method).await?;
        trace!("Redirect: Response {} location={:?}", 
            r.status(), r.headers().get(hyper::header::LOCATION) 
        );
        match redirect_filter(r) {
            Ok(b) => Err(app_error!(generic "Expected redirect, found non-redirect response status={}", b.status())),
            Err(e) => match e.to_http_redirect() {
                Ok((_code, location)) => match location.parse() {
                    Ok(uri) => Ok(HttpxEndpoint::new(natmap.translate(uri)?, https_settings)),
                    Err(e) => Err(app_error!((cause=e) "Cannot parse location URI returned by redirect"))
                }
                Err(e) => Err(e)
            }
        }
    }
    
    /// single-step request to nn (no redirects expected), no input, json output
    pub async fn get_json<R>(self) -> Result<R>
        where R: serde::de::DeserializeOwned + Send + 'static {
        let Self { endpoint, natmap:_ } = self;
        let result = HttpxClient::new_get_like(endpoint, Method::GET).await?;
        let result_filtered = error_and_ct_filter(RCT::JSON, result).await?;
        extract_json(result_filtered).await
    }

    /// single-step mutation request (no redirects expected), empty input, json output
    pub async fn op_json<R>(self, method: Method) -> Result<R> 
     where R: serde::de::DeserializeOwned + Send + 'static {
        let Self { endpoint, natmap: _ } = self;
        let result = HttpxClient::new_post_like(endpoint, method, data_empty()).await?;
        let result_filtered = error_and_ct_filter(RCT::JSON, result).await?;
        extract_json(result_filtered).await
    }

    /// single-step mutation request (no redirects expected), empty input, empty output
    pub async fn op_empty(self, method: Method) -> Result<()> {
        let Self { endpoint, natmap:_ } = self;
        let result = HttpxClient::new_post_like(endpoint, method, data_empty()).await?;
        let result_filtered = error_and_ct_filter(RCT::None, result).await?;
        extract_empty(result_filtered).await
    }
    

    /// two-step data retrieval request, no input, binary output.
    /// returns pointer
    pub async fn get_binary(self) -> Result<Box<dyn Stream<Item=Result<Bytes>> + Unpin>> {
        let Self { endpoint, natmap } = self;
        let uri = HttpyClient::redirect_uri(endpoint, Method::GET, natmap).await?;
        let result = HttpxClient::new_get_like(uri, Method::GET).await?;
        let r = error_and_ct_filter(RCT::Binary, result).await?;
        let xb = extract_binary(r).await;
        Ok(Box::new(xb))
    }

    /// two-step data submission request, data input, empty output. data returned back on error
    pub async fn post_binary(self, method: Method, data: Data) -> DResult<()> {
        async fn inner(endpoint: HttpxEndpoint, method: Method, data: Data) -> Result<()> {
            let result = HttpxClient::new_post_like(endpoint, method, data).await?;
            let result_filtered = error_and_ct_filter(RCT::None, result).await?;
            extract_empty(result_filtered).await
        }

        let Self { endpoint, natmap } = self;
        match HttpyClient::redirect_uri(endpoint, method.clone(), natmap).await {
            Ok(endpoint) => inner(endpoint, method, data).map(|fr| fr.map_err(ErrorD::lift)).await,
            Err(e) => Err(ErrorD::d(e, data))
        }
    }
}




//-----------------------------------------------------------------------------------------------------------
/*
#[cfg(test)]
mod client_tests {
    use crate::rest_client::*;
    use futures::Future;
    use tokio::runtime::Runtime;
    use serde::{Serialize, Deserialize};

    fn f_wait<I, E>(f: impl Future<Item=I, Error=E> + Send + 'static) -> StdResult<I,E>
        where I: Send + 'static, E: Send + 'static {
        Runtime::new().unwrap().block_on(f)
    }

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    struct R {
        operation: String,
        expression: String,
        result: String
    }

    
    #[test]
    fn test_get_s() {
        let uri = "https://newton.now.sh/factor/x%5E2-1".parse().unwrap();
        let res = f_wait(HttpyClient::new(uri, NatMapPtr::empty()).get_json::<R>()).unwrap();
        assert_eq!(res , R {
            operation: "factor".to_string(),
            expression: "x^2-1".to_string(),
            result: "(x - 1) (x + 1)".to_string()
        });
        println!("{:?}", res);
    }
}
*/
