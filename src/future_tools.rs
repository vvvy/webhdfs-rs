use crate::*;
use std::future::Future;
use futures::{FutureExt, Stream, future::{err, Either}, stream::iter};

/// converts Result<Future<Result<T>>> into Future<Result<T>>
#[inline]
pub fn simplify_future_result<F, T>(result: Result<F>) -> impl Future<Output=Result<T>>
where F: Future<Output=Result<T>> {
    match result {
        Ok(v) => Either::Right(v),
        Err(e) => Either::Left(err(e))
    }
}

/// converts Result<Stream<Result<T>>> into Stream<Result<T>>
#[inline]
pub fn simplify_stream_result<F, T>(result: Result<F>) -> impl Stream<Item=Result<T>>
where F: Stream<Item=Result<T>> {
    match result {
        Ok(v) => Either::Right(v),
        Err(e) => Either::Left(iter(vec![Err(e)]))
    }
}

/// converts Future<Result<Stream<Result<T>>>> into Stream<Result<T>>
#[inline]
pub fn simplify_future_stream_result<F, T>(result_f: impl Future<Output=Result<F>>) -> impl Stream<Item=Result<T>>
where F: Stream<Item=Result<T>> {
    result_f.map(|r| simplify_stream_result(r)).flatten_stream()
}