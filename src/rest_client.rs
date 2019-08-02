use futures::{Future, Stream};

use http::{uri::Scheme, request::Builder as RequestBuilder};
use hyper::{
    Request, Response, Body, Uri, Chunk,
    client::{Client, ResponseFuture, HttpConnector}
};
use hyper_tls::HttpsConnector;
use mime::Mime;
use crate::error::*;
use crate::datatypes::RemoteException;
use crate::natmap::NatMapPtr;


#[inline]
fn content_type_extractor(res: Response<Body>) -> Result<(mime::Mime, Response<Body>)> {
    use std::str::FromStr;
    let m = res.headers()
        .get(hyper::header::CONTENT_TYPE)
        .map(|s| s.to_str().map(|x| mime::Mime::from_str(x)));
    match m {
        Some(Ok(Ok(ct))) => Ok((ct, res)),
        Some(Ok(Err(ect))) => Err(ect.into()),
        Some(Err(ect)) => Err(ect.into()),
        None => Err(app_error!(generic "no content type found (application/json or application/octet-stream required)"))
    }
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

#[derive(Debug, PartialEq, Copy, Clone)]
enum RCT {
    JSON,
    Binary
}

#[inline]
fn match_mimes(ct: &Mime, ct_required: RCT) -> bool {
    match ct_required {
        RCT::JSON => match (ct.type_(), ct.subtype(), ct.get_param("charset")) {
            (mime::APPLICATION, mime::JSON, Some(mime::UTF_8)) => true,
            (mime::APPLICATION, mime::JSON, None) => true,
            _ => false
        }
        RCT::Binary => mime::APPLICATION_OCTET_STREAM.eq(ct)
    }
}

#[inline]
fn error_and_ct_filter(ct: &Mime, ct_required: RCT, res: Response<Body>) -> Box<dyn Future<Item=Response<Body>, Error=Error> + Send> {
    use futures::future::{ok, err};
    if res.status().is_success() {
        if match_mimes(ct, ct_required)
        {
            Box::new(ok(res))
        } else {
            Box::new(err(app_error!(generic "Invald content type: required='{:?}' found='{}'", ct_required, ct)))
        }
    } else {
        let concat = res.into_body().concat2().from_err();
        if match_mimes(ct, RCT::JSON) {
            Box::new(
                concat.and_then(move |body| 
                    serde_json::from_slice::<RemoteException>(&body)
                        .aerr_f(|| format!("JSON err deseriaization error, recovered text: '{}'", String::from_utf8_lossy(body.as_ref())))
                ).and_then(|ex| 
                    futures::future::err(ex.into())
                )
            )
        } else {
            Box::new(
                concat.and_then(move |body|  
                    futures::future::err(app_error!(
                        generic "Remote error w/o JSON content, recovered text: '{}'", String::from_utf8_lossy(body.as_ref())
                    ))
                )
            )
        }
    }
}

#[inline]
fn ensure_ct(ct_required: RCT, f: impl Future<Item=Response<Body>, Error=Error> + Send) 
-> impl Future<Item=Response<Body>, Error=Error> + Send {
    f.and_then(move |res| 
        match content_type_extractor(res) {
            Ok((ct, res)) => error_and_ct_filter(&ct, ct_required, res),
            Err(e) => Box::new(futures::future::err(e))
        }
    )
}

#[inline]
fn extract_json<R>(f: impl Future<Item=Response<Body>, Error=Error> + Send) 
-> impl Future<Item=R, Error=Error> + Send
where R: serde::de::DeserializeOwned + Send {
    f.and_then(|res|
        res.into_body().concat2().from_err().and_then(|body| serde_json::from_slice(&body).aerr("JSON deseriaization error"))
    )
}

#[inline]
fn extract_binary(f: impl Future<Item=Response<Body>, Error=Error> + Send) 
-> impl Stream<Item=Chunk, Error=Error> + Send {
    f.map(|res|res.into_body().from_err()).flatten_stream()
}

//---------------


fn extract_content_type(res: &Response<Body>) -> Result<mime::Mime> {
    use std::str::FromStr;
    let m = res.headers()
        .get(hyper::header::CONTENT_TYPE)
        .map(|s| s.to_str().map(|x| mime::Mime::from_str(x)));
    match m {
        Some(Ok(Ok(ct))) => Ok(ct),
        Some(Ok(Err(ect))) => Err(ect.into()),
        Some(Err(ect)) => Err(ect.into()),
        None => Err(app_error!(generic "no content type found (application/json or application/octet-stream required)"))
    }
}

fn extract_json_body<R>(res: Response<Body>) -> Box<dyn Future<Item=R, Error=Error> + Send>
    where R: serde::de::DeserializeOwned + Send + 'static {
    let status = res.status();
    let concat = res.into_body().concat2().from_err();

    if status.is_success() {
        Box::new(
            concat.and_then(|body| 
                serde_json::from_slice(&body).aerr("JSON deseriaization error")
            )
        )
    } else {
        Box::new(
            concat.and_then(move |body| 
                serde_json::from_slice::<RemoteException>(&body).aerr("JSON err deseriaization error")
            ).and_then(|ex| 
                futures::future::err(ex.into())
            )
        )
    }
}

fn http_process_json_response<R>(
    rf: ResponseFuture
) -> impl Future<Item=R, Error=Error> + Send
    where R: serde::de::DeserializeOwned + Send + 'static {
    use futures::future::err;

    rf
        .from_err()
        .and_then(|res| {
            match redirect_filter(res) {
                Err(e) => Box::new(err(e)),
                Ok(res) => {
                    match extract_content_type(&res) {
                        Ok(ref ct) if ct.type_() == mime::APPLICATION && ct.subtype() == mime::JSON => 
                            extract_json_body(res),
                        Ok(ct) => 
                            Box::new(err(app_error!(generic "invalid content type `{}` (application/json expected)", ct))),
                        Err(e) => 
                            Box::new(err(e)),
                    }
                }
            }
        })
}



fn http_attach_json_request_data<Q>(mut req: RequestBuilder, q: &Q)-> Result<Request<Body>>
    where Q: serde::ser::Serialize
{
    req.header(hyper::header::CONTENT_TYPE, mime::APPLICATION_JSON.as_ref());
    let data =  serde_json::to_vec(q)?;
    //let dlen = data.len();
    //req.header(hyper::header::CONTENT_LENGTH, dlen as u64);
    let body: Body = data.into();
    let rv = req.body(body)?;
    Ok(rv)
}

#[inline]
fn http_empty_body(mut request: RequestBuilder) -> Result<Request<Body>> {
    Ok(request.body(Body::empty())?)
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

pub struct HttpxClient {
    endpoint: Httpx,
}

impl HttpxClient
{
    pub fn new(uri: &Uri) -> Result<HttpxClient> {
        Httpx::new(uri).map(|p| HttpxClient { endpoint: p })
    }

    #[inline]
    fn create_request(&self, method: http::method::Method, uri: Uri) -> RequestBuilder {
        let mut r = RequestBuilder::new();
        r.method(method).uri(uri);
        r
    }

    #[inline]
    fn get_future(&self, uri: Uri) -> impl Future<Item=Response<Body>, Error=Error> + Send {
        let r = self.create_request(http::method::Method::GET, uri);
        let f = http_empty_body(r).map(|r| self.endpoint.request_raw(r).from_err());
        futures::future::result(f).flatten()
    }

    fn new_get(uri: Uri) -> Box<dyn Future<Item=Response<Body>, Error=Error> + Send> {
        match Self::new(&uri) {
            Ok(c) => Box::new(c.get_future(uri)),
            Err(e) => Box::new(futures::future::err(e))
        }
    }

    fn new_get_with_redirect(uri: Uri, natmap: NatMapPtr) -> impl Future<Item=Response<Body>, Error=Error> + Send {
        fn handle_redirect(r: Result<Response<Body>>, natmap: NatMapPtr) -> Box<dyn Future<Item=Response<Body>, Error=Error> + Send> {
            use futures::future::{ok, err};
            match r {
                Ok(r) => Box::new(ok(r)),
                Err(e) => match e.to_http_redirect() {
                    Ok((_code, location)) => match location.parse() {
                        Ok(uri) => match natmap.translate(uri) { 
                            Ok(uri) => HttpxClient::new_get(uri),
                            Err(e) => Box::new(err(e))
                        }
                        Err(e) => Box::new(err(app_error!((cause=e) "Cannot parse location URI returned by redirect")))
                    }
                    Err(e) => Box::new(err(e))
                }
            }
        }
        Self::new_get(uri)
            .and_then(|r| redirect_filter(r))
            .then(|r| handle_redirect(r, natmap))
    }

    
    pub fn new_get_json<R>(uri: Uri, natmap: NatMapPtr) -> impl Future<Item=R, Error=Error> + Send
        where R: serde::de::DeserializeOwned + Send + 'static {
        
        let f0 = Self::new_get_with_redirect(uri, natmap);
        let f1 = ensure_ct(RCT::JSON, f0);
        let f2 = extract_json(f1);
        f2
    }

    pub fn new_get_binary(uri: Uri, natmap: NatMapPtr) -> impl Stream<Item=Chunk, Error=Error> + Send {
        let f0 = Self::new_get_with_redirect(uri, natmap);
        let f1 = ensure_ct(RCT::Binary, f0);
        let f2 = extract_binary(f1);
        f2
    }

    pub fn post<Q, R>(&self, uri: Uri, q: &Q) -> impl Future<Item=R, Error=Error> + Send
        where Q: serde::ser::Serialize,
              R: serde::de::DeserializeOwned + Send + 'static
    {
        let r = self.create_request(http::method::Method::POST, uri);
        let f = http_attach_json_request_data(r, q)
            .map(|r| self.endpoint.request_raw(r));
        futures::future::result(f).and_then(|e| http_process_json_response(e))
    }

}




//-----------------------------------------------------------------------------------------------------------

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
        let res = f_wait(HttpxClient::new_get_json::<R>(uri, NatMapPtr::empty())).unwrap();
        assert_eq!(res , R {
            operation: "factor".to_string(),
            expression: "x^2-1".to_string(),
            result: "(x - 1) (x + 1)".to_string()
        });
        println!("{:?}", res);
    }


    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    struct PubReq {
        title: String,
        body: String,
        #[serde(rename="userId")]
        user_id: i32
    }

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    struct PubResp {
        title: String,
        body: String,
        #[serde(rename="userId")]
        user_id: i32,
        id: i32
    }

    #[test]
    fn test_post_s() {
        let uri = "https://jsonplaceholder.typicode.com/posts".parse().unwrap();
        let cl = HttpxClient::new(&uri).unwrap();
        let res = f_wait(cl.post::<PubReq, PubResp>(
            uri,
            &PubReq { title: "ABC".to_string(), body: "DEF".to_string(), user_id: 111 }
        )).unwrap();
        assert_eq!(res , PubResp {
            title: "ABC".to_string(), body: "DEF".to_string(), user_id: 111,
            id: 101
        });
        println!("{:?}", res);
    }

    #[test]
    fn test_post() {
        let uri = "http://jsonplaceholder.typicode.com/posts".parse().unwrap();
        let cl = HttpxClient::new(&uri).unwrap();
        let res = f_wait(cl.post::<PubReq, PubResp>(
            uri,
            &PubReq { title: "ABC".to_string(), body: "DEF".to_string(), user_id: 111 }
        )).unwrap();
        assert_eq!(res , PubResp {
            title: "ABC".to_string(), body: "DEF".to_string(), user_id: 111,
            id: 101
        });
        println!("{:?}", res);
    }

}
