use hyper;
use hyper_tls;
use mime;
use serde_json;
use http;
use std;
use std::borrow::Cow;
use std::fmt::{Display, Formatter, Result as FmtResult};

pub use std::result::Result as StdResult;
pub type Result<T> = StdResult<T, Error>;

#[derive(Debug)]
pub enum Cause {
    None,
    Hyper(hyper::error::Error),
    HyperHeaderToStr(hyper::header::ToStrError),
    HyperTls(hyper_tls::Error),
    MimeFromStr(mime::FromStrError),
    SerdeJson(serde_json::Error),
    Http(http::Error),
    HttpInvalidUri(http::uri::InvalidUri),
    HttpInvalidUriParts(http::uri::InvalidUriParts),
    Io(std::io::Error),
    RemoteException(crate::datatypes::RemoteException),
    HttpRedirect(u16, String)
}

#[derive(Debug)]
pub struct Error {
    msg: Option<Cow<'static, str>>,
    cause: Cause
}

impl Error {
    pub fn new(msg: Option<Cow<'static, str>>, cause: Cause) -> Self { Error { msg, cause } }
    pub fn anon(cause: Cause) -> Self { Self::new(None, cause) }
    pub fn with_msg_prepended(self, msg: Cow<'static, str>) -> Self {        
        Error { 
            msg: Some(match self.msg {
                Some(m) => msg + "\n" + m,
                None => msg
            }),
            cause: self.cause 
        }
    }
    pub fn app_c(msg: &'static str) -> Self { Error::new(Some(Cow::Borrowed(msg)), Cause::None) }
    pub fn app_s(msg: String) -> Self { Error::new(Some(Cow::Owned(msg)), Cause::None) }
    pub fn msg_s(&self) -> &str {
        match &self.msg {
            Some(m) => &m,
            None => "GENERIC"
        }
    }
    pub fn to_http_redirect(self) -> Result<(u16, String)> {
        match self.cause {
            Cause::HttpRedirect(code, location) => Ok((code, location)),
            other => Err(Self::new(self.msg, other))
        }
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "AppError: {}", self.msg_s())?;
        match &self.cause {
            Cause::Hyper(e) => write!(f, "; caused by hyper::error::Error: {}", e),
            Cause::HyperHeaderToStr(e) => write!(f, "; caused by hyper::header::ToStrError: {}", e),
            Cause::HyperTls(e) => write!(f, "; caused by hyper_tls::Error: {}", e),
            Cause::MimeFromStr(e) => write!(f, "; caused by mime::FromStrError: {}", e),
            Cause::SerdeJson(e) => write!(f, "; caused by serde_json::Error: {}", e),
            Cause::Http(e) => write!(f, "; caused by http::Error: {}", e),
            Cause::HttpInvalidUri(e) => write!(f, "; caused by http::uri::InvalidUri: {}", e),
            Cause::HttpInvalidUriParts(e) => write!(f, "; caused by http::uri::InvalidUriParts: {}", e),
            Cause::Io(e) => write!(f, "; caused by IoError: {}", e),
            Cause::RemoteException(e) => write!(f, "; caused by RemoteException {}", e),
            Cause::HttpRedirect(code, location) => write!(f, "; caused by HTTP redirect {} {}", code, location),
            Cause::None => Ok(())
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match &self.cause {
            Cause::Hyper(e) => Some(e),
            Cause::HyperHeaderToStr(e) => Some(e),
            Cause::HyperTls(e) => Some(e),
            Cause::MimeFromStr(e) => Some(e),
            Cause::SerdeJson(e) => Some(e),
            Cause::Http(e) => Some(e),
            Cause::HttpInvalidUri(e) => Some(e),
            Cause::HttpInvalidUriParts(e) => Some(e),
            Cause::Io(e) => Some(e),
            Cause::RemoteException(e) => Some(e),
            Cause::HttpRedirect(_, _) => None,
            Cause::None => None
        }
    }
}


#[cfg(panic_on_error)]
macro_rules! app_error {
    (generic $s:expr, $($arg:expr),+) => { panic!(format!($s,$($arg),+)) };
    (generic $s:expr) => { panic!($s) };
    ((cause=$c:expr) $s:expr, $($arg:expr),+) => { panic!($c.into_with(std::borrow::Cow::Owned(format!($s,$($arg),+))).to_string()) };
    ((cause=$c:expr) $s:expr) => { panic!($c.into_with(std::borrow::Cow::Borrowed($s)).to_string()) };
}

#[cfg(not(panic_on_error))]
macro_rules! app_error {
    (generic $s:expr, $($arg:expr),+) => { crate::error::Error::app_s(format!($s,$($arg),+)) };
    (generic $s:expr) => { crate::error::Error::app_c($s) };
    ((cause=$c:expr) $s:expr, $($arg:expr),+) => { $c.into_with(std::borrow::Cow::Owned(format!($s,$($arg),+))) };
    ((cause=$c:expr) $s:expr) => { $c.into_with(std::borrow::Cow::Borrowed($s)) };
}

pub trait IntoErrorAnnotated: Sized {
    fn into_with(self, msg: Cow<'static, str>) -> Error;
    fn into_with_c(self, msg: &'static str) -> Error { self.into_with(Cow::Borrowed(msg)) }
    fn into_with_s(self, msg: String) -> Error { self.into_with(Cow::Owned(msg)) }
}

impl IntoErrorAnnotated for Error {
    fn into_with(self, msg: Cow<'static, str>) -> Error { self.with_msg_prepended(msg) }
}

pub trait AnnotateError<T>: Sized {
    /// a shortcut for `.map_err(|x| app_err((cause=x) "...")
    fn aerr(self, msg: &'static str) -> Result<T>;
    /// a shortcut for `.map_err(|x| app_err((cause=x) msg), with msg lazily evaluated
    fn aerr_f(self, msg_f: impl FnOnce() -> String) -> Result<T>;
}

impl<T, E> AnnotateError<T> for std::result::Result<T, E> where E: IntoErrorAnnotated {
    fn aerr(self, msg: &'static str) -> Result<T> {
        self.map_err(|e| e.into_with(Cow::Borrowed(msg)))
    }
    fn aerr_f(self, msg_f: impl FnOnce() -> String) -> Result<T> {
        self.map_err(|e| e.into_with(Cow::Owned(msg_f())))
    }
} 

macro_rules! error_conversion {
    ($f:ident($t:ty)) => {
        impl From<$t> for Error {
            #[cfg(panic_on_error)]
            fn from(e: $t) -> Self {  panic!(Error::anon(Cause::$f(e)).to_string()) }
            #[cfg(not(panic_on_error))]
            fn from(e: $t) -> Self {  Error::anon(Cause::$f(e)) }
        }

        impl IntoErrorAnnotated for $t {
            fn into_with(self, msg: Cow<'static, str>) -> Error {
                Error::new(Some(msg), Cause::$f(self))
            }
        }
    };
}

macro_rules! error_conversions {
    ($($f:ident($t:ty)),+) => { $(error_conversion!{$f($t)})+ } 
}

error_conversions!{
    Hyper(hyper::error::Error),
    HyperHeaderToStr(hyper::header::ToStrError),
    HyperTls(hyper_tls::Error),
    MimeFromStr(mime::FromStrError),
    SerdeJson(serde_json::Error),
    Http(http::Error),
    HttpInvalidUri(http::uri::InvalidUri),
    HttpInvalidUriParts(http::uri::InvalidUriParts),
    Io(std::io::Error),
    RemoteException(crate::datatypes::RemoteException)
}

impl From<Error> for std::io::Error {
    fn from(e: Error) -> Self {
        std::io::Error::new(std::io::ErrorKind::Other, e)
    }
}
