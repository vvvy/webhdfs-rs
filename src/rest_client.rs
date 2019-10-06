use futures::{Future, Stream};

use http::{uri::Scheme, request::Builder as RequestBuilder, method::Method};
use hyper::{
    Request, Response, Body, Uri,
    client::{Client, ResponseFuture, HttpConnector}
};
use bytes::Bytes;
use hyper_tls::HttpsConnector;
use mime::Mime;
use log::trace;
use crate::error::*;
use crate::datatypes::RemoteExceptionResponse;
use crate::natmap::NatMapPtr;


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
fn error_and_ct_filter(ct_required: RCT, res: Response<Body>) -> Box<dyn Future<Item=Response<Body>, Error=Error> + Send> {
    use futures::future::{ok, err};

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

    match content_type_extractor(&res) {
        Ok(ct) => if res.status().is_success() { 
            if match_mimes(&ct, ct_required) {
                Box::new(ok(res))
            } else {
                Box::new(err(app_error!(generic "Invald content type: required='{:?}' found='{:?}'", ct_required, ct)))
            }
        } else {
            let concat = res.into_body().concat2().from_err();
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
        }
        Err(e) => Box::new(err(e))
    }
}

#[inline]
fn ensure_ct(ct_required: RCT, f: impl Future<Item=Response<Body>, Error=Error> + Send) 
-> impl Future<Item=Response<Body>, Error=Error> + Send {
    f.and_then(move |res| error_and_ct_filter(ct_required, res))
}

#[inline]
fn extract_json<R>(f: impl Future<Item=Response<Body>, Error=Error> + Send) -> impl Future<Item=R, Error=Error> + Send
where R: serde::de::DeserializeOwned + Send {
    f.and_then(|res| {
        trace!("HTTP JSON Response {} ct={:?} cl={:?}", res.status(), res.headers().get(hyper::header::CONTENT_TYPE), res.headers().get(hyper::header::CONTENT_LENGTH));
        res.into_body().concat2().from_err().and_then(|body| serde_json::from_slice(&body).aerr("JSON deseriaization error"))
    })
}

#[inline]
fn extract_binary(f: impl Future<Item=Response<Body>, Error=Error> + Send) -> impl Stream<Item=Bytes, Error=Error> + Send {
    f.map(|res| {
        trace!("HTTP Binary Response {} ct={:?} cl={:?}", res.status(), res.headers().get(hyper::header::CONTENT_TYPE), res.headers().get(hyper::header::CONTENT_LENGTH));
        res.into_body().from_err()
    }).flatten_stream().map(|c| c.into_bytes())
}

#[inline]
fn extract_empty(f: impl Future<Item=Response<Body>, Error=Error> + Send) -> impl Future<Item=(), Error=Error> + Send {
    f.and_then(|res| {
        trace!("HTTP Empty Response {} ct={:?} cl={:?}", res.status(), res.headers().get(hyper::header::CONTENT_TYPE), res.headers().get(hyper::header::CONTENT_LENGTH));
        res.into_body().concat2().from_err().and_then(|body| 
            if body.is_empty() {
                futures::future::ok(())
            } else {
                futures::future::err(app_error!(generic "Unexpected non-empty response received, where empty is expected"))
            }
        )
    })
}

#[inline]
fn http_empty_body(mut request: RequestBuilder) -> Result<Request<Body>> {
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
fn http_binary_body(mut request: RequestBuilder, payload: Data) -> Result<Request<Body>> {
    Ok(request.body(Body::from(payload))?)
}

enum Httpx {
    Http(Client<HttpConnector, Body>),
    Https(Client<HttpsConnector<HttpConnector>, Body>)
}

impl Httpx {
    fn new(uri: &Uri) -> Result<Httpx> {
        if Some(&Scheme::HTTPS) == uri.scheme_part() {
            Ok(HttpsConnector::new(1)
                .map(|connector|
                    Httpx::Https(Client::builder().build::<_, hyper::Body>(connector))
                )?)
        } else {
            Ok(Httpx::Http(Client::new()))
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
    endpoint: Httpx,
}

impl HttpxClient
{
    fn new(uri: &Uri) -> Result<HttpxClient> {
        Httpx::new(uri).map(|p| HttpxClient { endpoint: p })
    }

    #[inline]
    fn create_request(&self, method: Method, uri: Uri) -> RequestBuilder {
        trace!("{} {}", method, uri);
        let mut r = RequestBuilder::new();
        r.method(method).uri(uri);
        r
    }

    #[inline]
    fn get_like_future(&self, uri: Uri, method: Method) -> impl Future<Item=Response<Body>, Error=Error> + Send {
        let r = self.create_request(method, uri);
        let f = http_empty_body(r).map(|r| self.endpoint.request_raw(r).from_err());
        futures::future::result(f).flatten()
    }

    #[inline]
    fn post_like_future(&self, uri: Uri, method: Method, payload: Data) -> impl Future<Item=Response<Body>, Error=Error> + Send {
        let r = self.create_request(method, uri);
        let f = http_binary_body(r, payload).map(|r| self.endpoint.request_raw(r).from_err());
        futures::future::result(f).flatten()
    }

    fn new_get_like(uri: Uri, method: Method) -> Box<dyn Future<Item=Response<Body>, Error=Error> + Send> {
        match Self::new(&uri) {
            Ok(c) => Box::new(c.get_like_future(uri, method)),
            Err(e) => Box::new(futures::future::err(e))
        }
    }

    fn new_post_like(uri: Uri, method: Method, payload: Data) -> Box<dyn Future<Item=Response<Body>, Error=Error> + Send> {
        match Self::new(&uri) {
            Ok(c) => Box::new(c.post_like_future(uri, method, payload)),
            Err(e) => Box::new(futures::future::err(e))
        }
    }
}


pub struct HttpyClient {
    uri: Uri, 
    natmap: NatMapPtr
}

impl HttpyClient {
    pub fn new(uri: Uri, natmap: NatMapPtr) -> Self { Self { uri, natmap } }

    #[inline]
    fn request_with_redirect(
        self, 
        rf0: impl FnOnce(Uri) -> Box<dyn Future<Item=Response<Body>, Error=Error> + Send> + Send,
        rf1: impl FnOnce(Uri) -> Box<dyn Future<Item=Response<Body>, Error=Error> + Send> + Send
        ) -> impl Future<Item=Response<Body>, Error=Error> + Send {

        fn handle_redirect(
            r: Result<Response<Body>>, 
            natmap: NatMapPtr,
            rf1: impl FnOnce(Uri) -> Box<dyn Future<Item=Response<Body>, Error=Error> + Send>
            ) -> Box<dyn Future<Item=Response<Body>, Error=Error> + Send> {
            use futures::future::{ok, err};
            match r {
                Ok(r) => Box::new(ok(r)),
                Err(e) => match e.to_http_redirect() {
                    Ok((_code, location)) => match location.parse() {
                        Ok(uri) => match natmap.translate(uri) { 
                            Ok(uri) => rf1(uri),
                            Err(e) => Box::new(err(e))
                        }
                        Err(e) => Box::new(err(app_error!((cause=e) "Cannot parse location URI returned by redirect")))
                    }
                    Err(e) => Box::new(err(e))
                }
            }
        }
        let Self { uri, natmap } = self;
        rf0(uri)
            .and_then(|r| redirect_filter(r))
            .then(|r| handle_redirect(r, natmap, rf1))
    }
    
    pub fn get_json<R>(self) -> impl Future<Item=R, Error=Error> + Send
        where R: serde::de::DeserializeOwned + Send + 'static {
        
        let f0 = self.request_with_redirect( 
            |uri| HttpxClient::new_get_like(uri, Method::GET), 
            |uri| HttpxClient::new_get_like(uri, Method::GET)
        );
        let f1 = ensure_ct(RCT::JSON, f0);
        let f2 = extract_json(f1);
        f2
    }

    pub fn get_binary(self) -> impl Stream<Item=Bytes, Error=Error> + Send {
        let f0 = self.request_with_redirect( 
            |uri| HttpxClient::new_get_like(uri, Method::GET), 
            |uri| HttpxClient::new_get_like(uri, Method::GET)
        );
        let f1 = ensure_ct(RCT::Binary, f0);
        let f2 = extract_binary(f1);
        f2
    }

    pub fn post_binary(self, method: Method, data: Data) -> impl Future<Item=(), Error=Error> + Send {
        let method1 = method.clone();
        let f0 = self.request_with_redirect(
            |uri| HttpxClient::new_get_like(uri, method1), 
            move |uri| HttpxClient::new_post_like(uri, method, data)
        );
        let f1 = ensure_ct(RCT::None, f0);
        let f2 = extract_empty(f1);
        f2
    }

    /// No input, JSON output
    pub fn op_json<R>(self, method: Method) -> impl Future<Item=R, Error=Error> + Send 
     where R: serde::de::DeserializeOwned + Send + 'static{
        let method1 = method.clone(); 
        let f0 = self.request_with_redirect(
            |uri| HttpxClient::new_get_like(uri, method1), 
            move |uri| HttpxClient::new_post_like(uri, method, data_empty())
        );
        let f1 = ensure_ct(RCT::JSON, f0);
        let f2 = extract_json(f1);
        f2
    }

    /// No input, no output
    pub fn op_empty(self, method: Method) -> impl Future<Item=(), Error=Error> + Send {
        self.post_binary(method, data_empty())
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
