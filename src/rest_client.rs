use futures::{self, Future, Stream};
use http::uri::Scheme;
use hyper::{
    self,
    Request, Response, Body, Uri,
    client::{Client, ResponseFuture, HttpConnector}
};
use hyper_tls::HttpsConnector;
use serde_json;
use mime;
use crate::error::*;
use http::{self, request::Builder as RequestBuilder};
//use rest_auth;


fn extract_content_type(res: &Response<Body>) -> Result<mime::Mime> {
    use std::str::FromStr;
    let m = res.headers()
        .get(hyper::header::CONTENT_TYPE)
        .map(|s| s.to_str().map(|x| mime::Mime::from_str(x)));
    match m {
        Some(Ok(Ok(ct))) => Ok(ct),
        Some(Ok(Err(ect))) => Err(ect.into()),
        Some(Err(ect)) => Err(ect.into()),
        None => Err(app_error!(generic "no content type found (application/json or binary required)"))
    }
}

fn http_process_json_response<R>(
    rf: ResponseFuture
) -> impl Future<Item=R, Error=Error> + Send
    where R: serde::de::DeserializeOwned + Send {
    rf
        .map_err(|err| err.into())
        .and_then(|res| {
            match extract_content_type(&res) {
                Ok(ref ct) if ct.type_() == mime::APPLICATION && ct.subtype() == mime::JSON => futures::future::ok(res),
                Ok(ct) => futures::future::err(app_error!(generic "invalid content type `{}`", ct)),
                Err(e) => futures::future::err(e)
            }
        })
        .and_then(|res|
            res.into_body().concat2().map_err(|err| err.into())
        )
        .and_then(|body|
            serde_json::from_slice(&body).map_err(|err| err.into())
        )
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
    //auth: Box<Fn(&mut RequestBuilder) + Send>,
}

impl HttpxClient
{
    pub fn new(uri: &Uri) -> Result<HttpxClient> {
        Httpx::new(uri).map(|p| HttpxClient { endpoint: p })
    }

    /*
    pub fn new(uri: &Uri, auth: Option<(String, String)>) -> Result<HttpxClient, Error> {
        let mut a = rest_auth::BasicAuth::new("rest-service-realm")?;
        for (u, p) in auth {
            a.add_user(&u, &p);
        }
        Httpx::new(uri).map(|p| HttpxClient { endpoint: p, auth: a.run_client() })
    }
    */

    #[inline]
    fn create_request(&self, method: http::method::Method, uri: Uri) -> RequestBuilder {
        let mut r = RequestBuilder::new();
        r.method(method).uri(uri);
        //(self.auth)(&mut r);
        r
    }

    pub fn get<R>(&self, uri: Uri) -> impl Future<Item=R, Error=Error> + Send
        where R: serde::de::DeserializeOwned + Send
    {
        let r = self.create_request(http::method::Method::GET, uri);
        let f = http_empty_body(r).map(|r| self.endpoint.request_raw(r));
        futures::future::result(f).and_then(|e| http_process_json_response(e))
    }

    pub fn post<Q, R>(&self, uri: Uri, q: &Q) -> impl Future<Item=R, Error=Error> + Send
        where Q: serde::ser::Serialize,
              R: serde::de::DeserializeOwned + Send
    {
        let r = self.create_request(http::method::Method::POST, uri);
        let f = http_attach_json_request_data(r, q)
            .map(|r| self.endpoint.request_raw(r));
        futures::future::result(f).and_then(|e| http_process_json_response(e))
    }

}


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
        let cl = HttpxClient::new(&uri).unwrap();
        let res = f_wait(cl.get::<R>(uri)).unwrap();
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
