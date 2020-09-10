
use futures::{Stream, FutureExt, StreamExt};
use hyper::{
    Request, Response, Body, Uri,
    client::{Client, ResponseFuture, HttpConnector},
    body::aggregate
};
use hyper_tls::HttpsConnector;
use http::{uri::Scheme, request::Builder as RequestBuilder, method::Method};
use bytes::{Bytes, Buf};
use mime::Mime;
use log::trace;
use crate::error::*;
use crate::datatypes::RemoteExceptionResponse;
use crate::natmap::NatMapPtr;

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
    if res.status().is_success() {
        if match_mimes(&ct, ct_required) {
            Ok(res)
        } else {
            Err(app_error!(generic "Invald content type: required='{:?}' found='{:?}'", ct_required, ct))
        }
    } else {
        //Failure: try to retrieve JSON error message
        if match_mimes(&ct, RCT::JSON) {
            match aggregate(res.into_body()).await {
                Ok(buf) => match serde_json::from_slice::<RemoteExceptionResponse>(buf.bytes()) {
                    Ok(rer) => Err(rer.remote_exception.into()),
                    Err(e) => Err(app_error!(generic "JSON-error deseriaization error: {}, recovered text: '{}'", 
                        e, String::from_utf8_lossy(buf.bytes().as_ref())
                    ))
                }
                Err(e) => Err(app_error!(generic "JSON-error aggregation error: {}", e))
            }
        } else {
            Err(app_error!(generic "Remote error w/o JSON content"))
        }
    }
}

#[inline]
async fn extract_json<R>(res: Response<Body>) -> Result<R>
where R: serde::de::DeserializeOwned + Send { 
    trace!("HTTP JSON Response {} ct={:?} cl={:?}", 
        res.status(), res.headers().get(hyper::header::CONTENT_TYPE), res.headers().get(hyper::header::CONTENT_LENGTH)
    );
    let buf = aggregate(res.into_body()).await?;
    serde_json::from_slice(buf.bytes()).aerr("JSON deseriaization error")
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
    let buf = aggregate(res.into_body()).await?;
    if buf.bytes().is_empty() {
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

impl From<tokio::time::Elapsed> for ErrorD {
    fn from(e: tokio::time::Elapsed) -> Self { Self::lift(e.into()) }
}

/// Result with optional data recovered from error
pub type DResult<T> = StdResult<T, ErrorD>;

/// HTTP(S) client
enum Httpx {
    Http(Client<HttpConnector, Body>),
    Https(Client<HttpsConnector<HttpConnector>, Body>)
}

impl Httpx {
    fn new(uri: &Uri) -> Httpx {
        if Some(&Scheme::HTTPS) == uri.scheme() {
            let connector = HttpsConnector::new();
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
    fn new(uri: &Uri) -> Self { Self { endpoint: Httpx::new(uri) } }

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

    async fn new_get_like(uri: Uri, method: Method) -> Result<Response<Body>> {
        Self::new(&uri).get_like_future(uri, method).await
    }

    async fn new_post_like(uri: Uri, method: Method, payload: Data) -> Result<Response<Body>> {
        Self::new(&uri).post_like_future(uri, method, payload).await
    }
}

pub struct HttpyClient {
    uri: Uri, 
    natmap: NatMapPtr
}

impl HttpyClient {
    pub fn new(uri: Uri, natmap: NatMapPtr) -> Self { Self { uri, natmap } }

    #[inline]
    async fn redirect_uri(uri: Uri, method: Method, natmap: NatMapPtr)-> Result<Uri> {
        let r = HttpxClient::new_get_like(uri, method).await;
        match r {
            Ok(b) => Err(app_error!(generic "Expected redirect, found non-redirect response status={}", b.status())),
            Err(e) => match e.to_http_redirect() {
                Ok((_code, location)) => match location.parse() {
                    Ok(uri) => natmap.translate(uri),
                    Err(e) => Err(app_error!((cause=e) "Cannot parse location URI returned by redirect"))
                }
                Err(e) => Err(e)
            }
        }
    }
    
    /// single-step request to nn (no redirects expected), no input, json output
    pub async fn get_json<R>(self) -> Result<R>
        where R: serde::de::DeserializeOwned + Send + 'static {
        let Self { uri, natmap:_ } = self;
        let result = HttpxClient::new_get_like(uri, Method::GET).await?;
        let result_filtered = error_and_ct_filter(RCT::JSON, result).await?;
        extract_json(result_filtered).await
    }

    /// two-step data retrieval request, no input, binary output.
    /// returns pointer
    pub async fn get_binary(self) -> Result<Box<dyn Stream<Item=Result<Bytes>> + Unpin>> {
        let uri = HttpyClient::redirect_uri(self.uri, Method::GET, self.natmap).await?;
        let result = HttpxClient::new_get_like(uri, Method::GET).await?;
        let r = error_and_ct_filter(RCT::Binary, result).await?;
        let xb = extract_binary(r).await;
        Ok(Box::new(xb))
    }

    /// two-step data submission request, data input, empty output. data returned back on error
    pub async fn post_binary(self, method: Method, data: Data) -> DResult<()> {
        async fn inner(uri: Uri, method: Method, data: Data) -> Result<()> {
            let result = HttpxClient::new_post_like(uri, method, data).await?;
            let result_filtered = error_and_ct_filter(RCT::None, result).await?;
            extract_empty(result_filtered).await
        }

        let Self { uri, natmap } = self;
        match HttpyClient::redirect_uri(uri, method.clone(), natmap).await {
            Ok(uri) => inner(uri, method, data).map(|fr| fr.map_err(ErrorD::lift)).await,
            Err(e) => Err(ErrorD::d(e, data))
        }
    }

    /// two-step request, empty input, json output
    pub async fn op_json<R>(self, method: Method) -> Result<R> 
     where R: serde::de::DeserializeOwned + Send + 'static {
        let Self { uri, natmap } = self;
        let uri = HttpyClient::redirect_uri(uri, method.clone(), natmap).await?;
        let result = HttpxClient::new_post_like(uri, method, data_empty()).await?;
        let result_filtered = error_and_ct_filter(RCT::JSON, result).await?;
        extract_json(result_filtered).await
    }

    /// two-step request, empty input, empty output
    pub async fn op_empty(self, method: Method) -> Result<()> {
        let Self { uri, natmap } = self;
        let uri = HttpyClient::redirect_uri(uri, method.clone(), natmap).await?;
        let result = HttpxClient::new_post_like(uri, method, data_empty()).await?;
        let result_filtered = error_and_ct_filter(RCT::None, result).await?;
        extract_empty(result_filtered).await
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
