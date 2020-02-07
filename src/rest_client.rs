use std::future::Future;
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
use crate::future_tools;


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

    /*
    match content_type_extractor(&res) {
        Ok(ct) => if res.status().is_success() { 
            if match_mimes(&ct, ct_required) {
                Ok(res)
            } else {
                Err(app_error!(generic "Invald content type: required='{:?}' found='{:?}'", ct_required, ct))
            }
        } else {
            //TODO limit number of bytes read if ct not json (i.e. unknown)
            let maybe_bytes: Result<Vec<Bytes>> = res.into_body().fold(
                Ok(Vec::new()), 
                |acc, bres| async move { match (acc, bres) { (Ok(mut v), Ok(el)) => { v.push(el); Ok(v) } } }
            ).await;
            //let n = res.into_body().next();


            //impl Default for Result<Vec<Bytes>> {
            //    fn default() -> Result<Vec<Bytes>> { Ok(Vec::new())  }
            // }
            //let maybe_bytes: Vec<Result<Bytes>> = res.into_body().collect().await;

            match maybe_bytes {
                Ok(bytes) => if match_mimes(&ct, RCT::JSON) {
                    let ex = match serde_json::from_slice::<RemoteExceptionResponse>(&bytes) {
                        Ok(rer) => rer.remote_exception.into(),
                        Err(e) => app_error!(generic "JSON err deseriaization error, recovered text: '{}'", 
                            String::from_utf8_lossy(bytes.as_ref()))
                    };
                    Err(ex)    
                } else {
                    Err(app_error!(
                        generic "Remote error w/o JSON content, recovered text: '{}'", String::from_utf8_lossy(bytes.as_ref())
                    ))
                }
            }

            
            /*
            if match_mimes(&ct, RCT::JSON) {
                Box::new(
                    concat.and_then(move |body| 
                        serde_json::from_slice::<RemoteExceptionResponse>(&body)
                        .aerr_f(|| format!("JSON err deseriaization error, recovered text: '{}'", String::from_utf8_lossy(body.as_ref())))
                        .map(|er| er.remote_exception)

                    ).and_then(|ex| 
                        futures::future::err(ex.into())
                    )
                )
            } else {
                Box::new(
                    concat.and_then(move |body|  
                        err(app_error!(
                            generic "Remote error w/o JSON content, recovered text: '{}'", String::from_utf8_lossy(body.as_ref())
                        ))
                    )
                )
            }
            */
        }
        Err(e) => Err(e)
    }
    */
}

//#[inline]
//async fn ensure_ct(ct_required: RCT, f: impl Future<Output=Response<Body>> + Send) -> Result<Response<Body>> {
//    let res = f.await;
//    error_and_ct_filter(ct_required, res).await
//    //f.and_then(move |res| error_and_ct_filter(ct_required, res))
//}

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
async fn extract_binary(res: Response<Body>) -> impl Stream<Item=Result<Bytes>> {
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
    async fn request_with_redirect<X, Y>(
        self, 
        rf0: impl FnOnce(Uri) -> X,
        rf1: impl FnOnce(Uri) -> Y
        ) -> Result<Response<Body>> 
        where X: Future<Output=Result<Response<Body>>>, Y: Future<Output=Result<Response<Body>>> {

        async fn handle_redirect<X>(
            r: Result<Response<Body>>, 
            natmap: NatMapPtr,
            rf1: impl FnOnce(Uri) -> X
            ) -> Result<Response<Body>> 
            where X: Future<Output=Result<Response<Body>>> {
            match r {
                Ok(r) => Ok(r),
                Err(e) => match e.to_http_redirect() {
                    Ok((_code, location)) => match location.parse() {
                        Ok(uri) => match natmap.translate(uri) { 
                            Ok(uri) => rf1(uri).await,
                            Err(e) => Err(e)
                        }
                        Err(e) => Err(app_error!((cause=e) "Cannot parse location URI returned by redirect"))
                    }
                    Err(e) => Err(e)
                }
            }
        }
        let Self { uri, natmap } = self;
        let r0 = rf0(uri).await?;
        let r1 = redirect_filter(r0);
        handle_redirect(r1, natmap, rf1).await
    }
    
    pub async fn get_json<R>(self) -> Result<R>
        where R: serde::de::DeserializeOwned + Send + 'static {
        
        let result = self.request_with_redirect( 
            |uri| HttpxClient::new_get_like(uri, Method::GET), 
            |uri| HttpxClient::new_get_like(uri, Method::GET)
        ).await?;
        let result_filtered = error_and_ct_filter(RCT::JSON, result).await?;
        extract_json(result_filtered).await
    }

    pub fn get_binary(self) -> impl Stream<Item=Result<Bytes>> {
        #[inline]
        async fn binary_response(c: HttpyClient) -> Result<Response<Body>> {
            let result = c.request_with_redirect( 
                |uri| HttpxClient::new_get_like(uri, Method::GET), 
                |uri| HttpxClient::new_get_like(uri, Method::GET)
            ).await?;
            error_and_ct_filter(RCT::Binary, result).await
        }

        //Type: Future<Result<Future<Stream<Result>>>>
        let binary_stream_result = binary_response(self).map(|result| result.map(|resp| extract_binary(resp).flatten_stream()));

        future_tools::simplify_future_stream_result(binary_stream_result)      
    }

    pub async fn post_binary(self, method: Method, data: Data) -> Result<()> {
        let method1 = method.clone();
        let result = self.request_with_redirect(
            |uri| HttpxClient::new_get_like(uri, method1), 
            move |uri| HttpxClient::new_post_like(uri, method, data)
        ).await?;
        let result_filtered = error_and_ct_filter(RCT::None, result).await?;
        extract_empty(result_filtered).await
    }

    /// No input, JSON output
    pub async fn op_json<R>(self, method: Method) -> Result<R> 
     where R: serde::de::DeserializeOwned + Send + 'static{
        let method1 = method.clone(); 
        let result = self.request_with_redirect(
            |uri| HttpxClient::new_get_like(uri, method1), 
            move |uri| HttpxClient::new_post_like(uri, method, data_empty())
        ).await?;
        let result_filtered = error_and_ct_filter(RCT::JSON, result).await?;
        extract_json(result_filtered).await
    }

    /// No input, no output
    pub async fn op_empty(self, method: Method) -> Result<()> {
        self.post_binary(method, data_empty()).await
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
