/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! An HTTP server whose requests are answered by a Python actor.
//!
//! The server runs on Monarch's tokio runtime. Accepting connections, parsing,
//! routing, reading request bodies, writing responses, metrics and access logs
//! all happen here. A request that matches a route is sent to Python over a
//! [`pympsc`](crate::pympsc) channel as a [`PyHttpRequest`]; Python answers
//! through that object. `monarch._src.actor.http` is the Python half.
//!
//! Invariants:
//!
//! - **H-1 (no GIL on the runtime):** nothing in this module that runs on a
//!   tokio thread takes the GIL. Requests and signals reach Python through
//!   [`pympsc::Sender`], and Python's answers arrive over tokio channels.
//! - **H-2 (one answer):** each [`PyHttpRequest`] is answered at most once.
//!   A second answer raises in Python.
//! - **H-3 (client gone):** if a request's connection goes away before the
//!   server has taken a complete response for it, Python receives
//!   [`Signal::Disconnected`], so the handler can be cancelled. The signal can
//!   arrive after Python has answered; Python ignores signals for requests it
//!   has finished.
//! - **H-4 (shutdown):** after [`PyHttpServer::shutdown`] the server accepts
//!   no connection, lets requests in flight finish for the grace period, then
//!   fails the rest. [`Signal::Closed`] is the last thing Python receives.
//! - **H-5 (bounded response queue):** a response stream's writer is told to
//!   wait once [`STREAM_HIGH_WATER`] bytes are queued, and receives
//!   [`Signal::Drained`] when the queue falls to [`STREAM_LOW_WATER`].

use std::future::IntoFuture;
use std::io;
use std::net::IpAddr;
use std::net::SocketAddr;
use std::net::ToSocketAddrs;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;

use axum::Router;
use axum::body::Body;
use axum::extract::Request;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::http::HeaderName;
use axum::http::HeaderValue;
use axum::http::Method;
use axum::http::StatusCode;
use axum::http::Uri;
use axum::http::header;
use axum::response::Response;
use axum::serve::ListenerExt;
use bytes::Bytes;
use bytes::BytesMut;
use futures::StreamExt;
use hyperactor_telemetry::declare_static_counter;
use hyperactor_telemetry::declare_static_histogram;
use hyperactor_telemetry::declare_static_up_down_counter;
use monarch_types::MapPyErr;
use opentelemetry::KeyValue;
use pyo3::exceptions::PyBrokenPipeError;
use pyo3::exceptions::PyRuntimeError;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use pyo3::types::PyTuple;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::watch;

use crate::buffers::py_bytes_to_bytes;
use crate::pympsc;
use crate::pympsc::PyReceiver;
use crate::runtime::get_tokio_runtime;

/// Bytes queued on a response stream above which its writer is told to wait.
const STREAM_HIGH_WATER: usize = 1 << 20;

/// Bytes queued on a response stream at or below which a waiting writer is
/// told to continue.
const STREAM_LOW_WATER: usize = 1 << 18;

/// How long connections get to close once the grace period is over. Kept
/// short because actors usually stop the server from `__cleanup__`, which
/// Monarch bounds with `CLEANUP_TIMEOUT`.
const ABORT_TIMEOUT: Duration = Duration::from_secs(1);

/// The `status` metric attribute of a request whose client went away before a
/// response was produced.
const CLIENT_CLOSED: &str = "client_closed";

/// The `route` metric attribute of a request that matched no route.
const UNMATCHED: &str = "unmatched";

// Counts requests by actor, method, route and status.
declare_static_counter!(HTTP_SERVER_REQUESTS, "http_server_requests");
// Time from the request head arriving to the response being produced, in
// microseconds. For a streamed response this ends when the stream starts.
declare_static_histogram!(
    HTTP_SERVER_REQUEST_DURATION_US,
    "http_server_request_duration_us"
);
// Requests received and not yet answered, by actor.
declare_static_up_down_counter!(
    HTTP_SERVER_REQUESTS_IN_FLIGHT,
    "http_server_requests_in_flight"
);

/// What the server tells Python besides the requests themselves. Python
/// receives a signal as the tuple `(kind, request_id)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Signal {
    /// The request's client went away before its response was complete (H-3).
    Disconnected(u64),
    /// The request's response stream has room again (H-5).
    Drained(u64),
    /// The server has stopped; nothing follows (H-4).
    Closed,
}

impl Signal {
    const DISCONNECTED: u8 = 0;
    const DRAINED: u8 = 1;
    const CLOSED: u8 = 2;
}

impl<'py> IntoPyObject<'py> for Signal {
    type Target = PyTuple;
    type Output = Bound<'py, PyTuple>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Self::Output> {
        let pair = match self {
            Signal::Disconnected(id) => (Signal::DISCONNECTED, id),
            Signal::Drained(id) => (Signal::DRAINED, id),
            Signal::Closed => (Signal::CLOSED, 0),
        };
        pair.into_pyobject(py)
    }
}

/// One segment of a route's path pattern.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Segment {
    /// Matches this text exactly.
    Literal(String),
    /// Matches any non-empty segment and captures it under this name.
    Param(String),
}

/// A method and path pattern that Python handles.
#[derive(Debug)]
struct Route {
    method: Method,
    /// The pattern as registered, used as the `route` metric attribute.
    pattern: Arc<str>,
    segments: Vec<Segment>,
    /// How many segments are literal; the most specific match wins.
    literals: usize,
}

impl Route {
    fn new(method: &str, pattern: &str) -> Result<Self, String> {
        let method = Method::from_bytes(method.as_bytes())
            .map_err(|_| format!("invalid HTTP method {method:?}"))?;
        let segments = parse_pattern(pattern)?;
        let literals = segments
            .iter()
            .filter(|segment| matches!(segment, Segment::Literal(_)))
            .count();
        Ok(Self {
            method,
            pattern: pattern.into(),
            segments,
            literals,
        })
    }

    /// Whether `other` matches exactly the paths this route matches.
    fn same_paths(&self, other: &Route) -> bool {
        self.segments.len() == other.segments.len()
            && self
                .segments
                .iter()
                .zip(&other.segments)
                .all(|pair| match pair {
                    (Segment::Literal(a), Segment::Literal(b)) => a == b,
                    (Segment::Param(_), Segment::Param(_)) => true,
                    _ => false,
                })
    }

    /// The captured parameters if `path` matches this route's pattern.
    fn captures(&self, path: &[&str]) -> Option<Vec<(String, String)>> {
        if path.len() != self.segments.len() {
            return None;
        }
        let mut params = Vec::new();
        for (segment, value) in self.segments.iter().zip(path) {
            match segment {
                Segment::Literal(literal) if literal == value => {}
                Segment::Param(name) if !value.is_empty() => {
                    params.push((name.clone(), (*value).to_owned()));
                }
                _ => return None,
            }
        }
        Some(params)
    }
}

/// Parse a path pattern such as `/v1/models/{model}`.
fn parse_pattern(pattern: &str) -> Result<Vec<Segment>, String> {
    let Some(rest) = pattern.strip_prefix('/') else {
        return Err(format!("route path {pattern:?} must start with '/'"));
    };
    if pattern
        .chars()
        .any(|c| c == '?' || c == '#' || c.is_whitespace() || c.is_control())
    {
        return Err(format!(
            "route path {pattern:?} must not contain '?', '#', whitespace or control characters"
        ));
    }
    let mut segments = Vec::new();
    for text in rest.split('/') {
        let param = text
            .strip_prefix('{')
            .and_then(|text| text.strip_suffix('}'));
        let segment = match param {
            Some(name) => {
                let valid =
                    !name.is_empty() && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_');
                if !valid {
                    return Err(format!(
                        "route path {pattern:?} has an invalid parameter name {name:?}"
                    ));
                }
                if segments.contains(&Segment::Param(name.to_owned())) {
                    return Err(format!(
                        "route path {pattern:?} repeats the parameter {name:?}"
                    ));
                }
                Segment::Param(name.to_owned())
            }
            None if text.contains(['{', '}']) => {
                return Err(format!(
                    "route path {pattern:?} may only use braces as a whole segment, like '/{{name}}'"
                ));
            }
            None => Segment::Literal(text.to_owned()),
        };
        segments.push(segment);
    }
    Ok(segments)
}

/// The outcome of looking a request up in [`Routes`].
#[derive(Debug, PartialEq, Eq)]
enum Matched {
    /// The route at `index` handles the request.
    Route {
        index: usize,
        params: Vec<(String, String)>,
    },
    /// The path matches, but only for these methods.
    MethodNotAllowed(String),
    NotFound,
}

/// The routes of one server. A route's index is its position in the list
/// Python registered, which is how Python finds the handler.
#[derive(Debug)]
struct Routes(Vec<Route>);

impl Routes {
    fn new(routes: &[(String, String)]) -> Result<Self, String> {
        let mut parsed: Vec<Route> = Vec::with_capacity(routes.len());
        for (method, pattern) in routes {
            let route = Route::new(method, pattern)?;
            if let Some(existing) = parsed
                .iter()
                .find(|existing| existing.method == route.method && existing.same_paths(&route))
            {
                return Err(format!(
                    "route {} {} is already registered as {} {}",
                    route.method, route.pattern, existing.method, existing.pattern
                ));
            }
            parsed.push(route);
        }
        Ok(Self(parsed))
    }

    /// Find the route for `method` and `path`. Among routes that match, the
    /// one with the most literal segments wins, then the one registered
    /// first. `HEAD` falls back to a `GET` route.
    fn find(&self, method: &Method, path: &str) -> Matched {
        let Some(rest) = path.strip_prefix('/') else {
            return Matched::NotFound;
        };
        let path: Vec<&str> = rest.split('/').collect();
        let mut best: Option<(usize, Vec<(String, String)>)> = None;
        let mut fallback: Option<(usize, Vec<(String, String)>)> = None;
        let mut allowed: Vec<&Method> = Vec::new();
        for (index, route) in self.0.iter().enumerate() {
            let Some(params) = route.captures(&path) else {
                continue;
            };
            if !allowed.contains(&&route.method) {
                allowed.push(&route.method);
            }
            let slot = if route.method == *method {
                &mut best
            } else if *method == Method::HEAD && route.method == Method::GET {
                &mut fallback
            } else {
                continue;
            };
            let better = match slot {
                Some((current, _)) => route.literals > self.0[*current].literals,
                None => true,
            };
            if better {
                *slot = Some((index, params));
            }
        }
        match best.or(fallback) {
            Some((index, params)) => Matched::Route { index, params },
            None if allowed.is_empty() => Matched::NotFound,
            None => Matched::MethodNotAllowed(
                allowed
                    .iter()
                    .map(|method| method.as_str())
                    .collect::<Vec<_>>()
                    .join(", "),
            ),
        }
    }
}

/// A piece of a streamed response body, from Python to the connection.
enum Frame {
    Data(Bytes),
    /// The body is complete.
    End,
    /// The body cannot be completed; the connection is closed without a
    /// clean end so the client can tell.
    Abort,
}

/// Queue accounting shared by a response stream's writer and its connection
/// (H-5).
#[derive(Default)]
struct StreamShared {
    /// Bytes written and not yet taken by the connection.
    queued: AtomicUsize,
    /// Whether the writer was told to wait and is owed a wake-up.
    waiting: AtomicBool,
}

impl StreamShared {
    /// Account for `len` written bytes. Returns whether the writer must wait
    /// for [`Signal::Drained`].
    fn wrote(&self, len: usize) -> bool {
        let queued = self.queued.fetch_add(len, Ordering::SeqCst) + len;
        if queued <= STREAM_HIGH_WATER {
            return false;
        }
        self.waiting.store(true, Ordering::SeqCst);
        // The connection may have drained the queue before `waiting` was
        // set, and so sent no signal. Whoever clears `waiting` owns the
        // wake-up: if that is us, there is nothing to wait for.
        let drained = self.queued.load(Ordering::SeqCst) <= STREAM_LOW_WATER;
        !(drained && self.waiting.swap(false, Ordering::SeqCst))
    }

    /// Account for `len` bytes taken by the connection. Returns whether the
    /// writer is waiting and must be sent [`Signal::Drained`].
    fn took(&self, len: usize) -> bool {
        let queued = self.queued.fetch_sub(len, Ordering::SeqCst) - len;
        queued <= STREAM_LOW_WATER && self.waiting.swap(false, Ordering::SeqCst)
    }
}

/// Python's answer to a request.
enum Reply {
    Full {
        status: StatusCode,
        headers: HeaderMap,
        body: Bytes,
    },
    Stream {
        status: StatusCode,
        headers: HeaderMap,
        frames: mpsc::UnboundedReceiver<Frame>,
        shared: Arc<StreamShared>,
    },
}

fn status_and_headers(
    status: u16,
    headers: Vec<(String, String)>,
) -> PyResult<(StatusCode, HeaderMap)> {
    let status = StatusCode::from_u16(status)
        .map_err(|_| PyValueError::new_err(format!("invalid HTTP status {status}")))?;
    let mut map = HeaderMap::with_capacity(headers.len());
    for (name, value) in headers {
        let header_name = HeaderName::from_bytes(name.as_bytes())
            .map_err(|_| PyValueError::new_err(format!("invalid HTTP header name {name:?}")))?;
        let header_value = HeaderValue::from_str(&value).map_err(|_| {
            PyValueError::new_err(format!("invalid value for HTTP header {name:?}"))
        })?;
        map.append(header_name, header_value);
    }
    Ok((status, map))
}

/// A request waiting for Python's answer.
#[pyclass(
    name = "HttpRequest",
    module = "monarch._rust_bindings.monarch_hyperactor.http",
    frozen
)]
pub struct PyHttpRequest {
    id: u64,
    route: usize,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    params: Vec<(String, String)>,
    body: Bytes,
    /// Taken by the first answer (H-2).
    reply: Mutex<Option<oneshot::Sender<Reply>>>,
}

impl PyHttpRequest {
    fn take_reply(&self) -> PyResult<oneshot::Sender<Reply>> {
        self.reply
            .lock()
            .expect("reply lock is never held across a panic")
            .take()
            .ok_or_else(|| PyRuntimeError::new_err("this HTTP request was already answered"))
    }
}

#[pymethods]
impl PyHttpRequest {
    /// Identifies this request in the signals that follow it.
    #[getter]
    fn id(&self) -> u64 {
        self.id
    }

    /// Index of the matched route in the list the server was started with.
    #[getter]
    fn route(&self) -> usize {
        self.route
    }

    #[getter]
    fn method(&self) -> &str {
        self.method.as_str()
    }

    /// The path as sent, still percent-encoded.
    #[getter]
    fn path(&self) -> &str {
        self.uri.path()
    }

    /// The query string as sent, without the leading `?`; empty if absent.
    #[getter]
    fn query(&self) -> &str {
        self.uri.query().unwrap_or("")
    }

    /// Header names (lower-case) and values, in the order received.
    #[getter]
    fn headers(&self) -> Vec<(&str, String)> {
        self.headers
            .iter()
            .map(|(name, value)| {
                (
                    name.as_str(),
                    String::from_utf8_lossy(value.as_bytes()).into_owned(),
                )
            })
            .collect()
    }

    /// Names and values captured by the route's `{name}` segments, still
    /// percent-encoded.
    #[getter]
    fn path_params(&self) -> Vec<(String, String)> {
        self.params.clone()
    }

    #[getter]
    fn body<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, &self.body)
    }

    /// Answer with a complete body. Returns `False` if the client has gone.
    fn respond(
        &self,
        status: u16,
        headers: Vec<(String, String)>,
        body: Py<PyBytes>,
    ) -> PyResult<bool> {
        let (status, headers) = status_and_headers(status, headers)?;
        let reply = Reply::Full {
            status,
            headers,
            body: py_bytes_to_bytes(body),
        };
        Ok(self.take_reply()?.send(reply).is_ok())
    }

    /// Answer with a body written in pieces. Returns `None` if the client has
    /// gone.
    fn respond_stream(
        &self,
        status: u16,
        headers: Vec<(String, String)>,
    ) -> PyResult<Option<PyHttpBodyWriter>> {
        let (status, headers) = status_and_headers(status, headers)?;
        let (frames_tx, frames_rx) = mpsc::unbounded_channel();
        let shared = Arc::new(StreamShared::default());
        let reply = Reply::Stream {
            status,
            headers,
            frames: frames_rx,
            shared: Arc::clone(&shared),
        };
        Ok(self
            .take_reply()?
            .send(reply)
            .is_ok()
            .then_some(PyHttpBodyWriter {
                frames: frames_tx,
                shared,
            }))
    }
}

/// Writes a streamed response body.
#[pyclass(
    name = "HttpBodyWriter",
    module = "monarch._rust_bindings.monarch_hyperactor.http",
    frozen
)]
pub struct PyHttpBodyWriter {
    frames: mpsc::UnboundedSender<Frame>,
    shared: Arc<StreamShared>,
}

#[pymethods]
impl PyHttpBodyWriter {
    /// Queue `chunk` for the client. Returns `True` if the caller must wait
    /// for this request's drained signal before writing more (H-5). Raises
    /// `BrokenPipeError` if the body can no longer be written.
    fn write(&self, chunk: Py<PyBytes>) -> PyResult<bool> {
        let chunk = py_bytes_to_bytes(chunk);
        let len = chunk.len();
        if len == 0 {
            return Ok(false);
        }
        self.frames
            .send(Frame::Data(chunk))
            .map_err(|_| PyBrokenPipeError::new_err("the HTTP response stream is closed"))?;
        Ok(self.shared.wrote(len))
    }

    /// End the body normally.
    fn close(&self) {
        let _ = self.frames.send(Frame::End);
    }

    /// Give up on the body: the connection is closed without a clean end.
    fn abort(&self) {
        let _ = self.frames.send(Frame::Abort);
    }
}

/// Sends [`Signal::Disconnected`] when dropped, unless disarmed (H-3).
struct DisconnectGuard {
    id: u64,
    events: Option<pympsc::Sender>,
}

impl DisconnectGuard {
    fn new(id: u64, events: pympsc::Sender) -> Self {
        Self {
            id,
            events: Some(events),
        }
    }

    fn disarm(&mut self) {
        self.events = None;
    }
}

impl Drop for DisconnectGuard {
    fn drop(&mut self) {
        if let Some(events) = self.events.take() {
            let _ = events.send(Signal::Disconnected(self.id));
        }
    }
}

/// Records a request's metrics and access log line when dropped, so a request
/// whose client goes away is still counted.
struct RequestRecord {
    started: Instant,
    actor: Arc<str>,
    method: Method,
    /// Kept only when the access log line will be emitted.
    path: Option<String>,
    route: Option<Arc<str>>,
    status: Option<StatusCode>,
}

impl RequestRecord {
    fn new(actor: &Arc<str>, request: &Request) -> Self {
        HTTP_SERVER_REQUESTS_IN_FLIGHT.add(1, &[KeyValue::new("actor", Arc::clone(actor))]);
        Self {
            started: Instant::now(),
            actor: Arc::clone(actor),
            method: request.method().clone(),
            path: tracing::enabled!(tracing::Level::DEBUG).then(|| request.uri().path().to_owned()),
            route: None,
            status: None,
        }
    }
}

impl Drop for RequestRecord {
    fn drop(&mut self) {
        let duration_us = self.started.elapsed().as_micros() as f64;
        let status = self.status.map_or_else(
            || CLIENT_CLOSED.to_owned(),
            |status| status.as_str().to_owned(),
        );
        let actor = KeyValue::new("actor", Arc::clone(&self.actor));
        HTTP_SERVER_REQUESTS_IN_FLIGHT.add(-1, std::slice::from_ref(&actor));
        if let Some(path) = &self.path {
            tracing::debug!(
                actor = &*self.actor,
                method = self.method.as_str(),
                path = path.as_str(),
                status = status.as_str(),
                duration_us,
                "http request"
            );
        }
        let route = self.route.take().unwrap_or_else(|| Arc::from(UNMATCHED));
        let attributes = [
            actor,
            KeyValue::new("method", self.method.as_str().to_owned()),
            KeyValue::new("route", route),
            KeyValue::new("status", status),
        ];
        HTTP_SERVER_REQUESTS.add(1, &attributes);
        HTTP_SERVER_REQUEST_DURATION_US.record(duration_us, &attributes);
    }
}

/// Why a request body could not be read.
enum BodyError {
    TooLarge,
    Read,
}

/// Read a request body of at most `limit` bytes. A body that arrives in one
/// piece is returned without a copy.
async fn read_body(body: Body, limit: usize) -> Result<Bytes, BodyError> {
    let mut chunks = body.into_data_stream();
    let Some(first) = chunks.next().await else {
        return Ok(Bytes::new());
    };
    let first = first.map_err(|_| BodyError::Read)?;
    if first.len() > limit {
        return Err(BodyError::TooLarge);
    }
    let Some(second) = chunks.next().await else {
        return Ok(first);
    };
    let mut buffer = BytesMut::from(first);
    let mut next = Some(second);
    while let Some(chunk) = next {
        let chunk = chunk.map_err(|_| BodyError::Read)?;
        if buffer.len() + chunk.len() > limit {
            return Err(BodyError::TooLarge);
        }
        buffer.extend_from_slice(&chunk);
        next = chunks.next().await;
    }
    Ok(buffer.freeze())
}

/// A response the server produces without asking Python.
fn error_response(status: StatusCode, message: &'static str) -> Response {
    let mut response = Response::new(Body::from(format!(r#"{{"error":"{message}"}}"#)));
    *response.status_mut() = status;
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    response
}

/// Resolves once the server has given up on requests in flight (H-4), or the
/// server task has gone.
async fn aborted(abort: &mut watch::Receiver<bool>) {
    while !*abort.borrow() {
        if abort.changed().await.is_err() {
            return;
        }
    }
}

/// The connection's side of a streamed response body.
struct BodyStream {
    id: u64,
    frames: mpsc::UnboundedReceiver<Frame>,
    shared: Arc<StreamShared>,
    events: pympsc::Sender,
    abort: watch::Receiver<bool>,
    /// Armed until the body ends, so that a connection dropped mid-body
    /// cancels the handler writing it (H-3).
    guard: DisconnectGuard,
}

impl BodyStream {
    fn into_body(self) -> Body {
        Body::from_stream(futures::stream::unfold(Some(self), |stream| async move {
            let mut stream = stream?;
            let frame = tokio::select! {
                frame = stream.frames.recv() => frame,
                () = aborted(&mut stream.abort) => {
                    return Some((Err(io::Error::other("the HTTP server stopped")), None));
                }
            };
            match frame {
                Some(Frame::Data(chunk)) => {
                    if stream.shared.took(chunk.len()) {
                        let _ = stream.events.send(Signal::Drained(stream.id));
                    }
                    Some((Ok(chunk), Some(stream)))
                }
                Some(Frame::End) => {
                    stream.guard.disarm();
                    None
                }
                // Python gave up on the body, or dropped its writer without
                // ending it. Python already knows, so no signal is owed.
                Some(Frame::Abort) | None => {
                    stream.guard.disarm();
                    // hyper drops what it has buffered when the body
                    // fails. Yielding once first lets it flush the chunks
                    // already produced, so the client receives them.
                    tokio::task::yield_now().await;
                    Some((
                        Err(io::Error::other("the HTTP response body was abandoned")),
                        None,
                    ))
                }
            }
        }))
    }
}

struct ServerState {
    routes: Routes,
    events: pympsc::Sender,
    max_body_bytes: usize,
    next_id: AtomicU64,
    abort: watch::Receiver<bool>,
    /// The `actor` metric attribute.
    actor: Arc<str>,
}

impl ServerState {
    async fn dispatch(&self, request: Request, record: &mut RequestRecord) -> Response {
        let (parts, body) = request.into_parts();
        let (index, params) = match self.routes.find(&parts.method, parts.uri.path()) {
            Matched::Route { index, params } => (index, params),
            Matched::MethodNotAllowed(allow) => {
                let mut response =
                    error_response(StatusCode::METHOD_NOT_ALLOWED, "method not allowed");
                if let Ok(allow) = HeaderValue::from_str(&allow) {
                    response.headers_mut().insert(header::ALLOW, allow);
                }
                return response;
            }
            Matched::NotFound => return error_response(StatusCode::NOT_FOUND, "not found"),
        };
        record.route = Some(Arc::clone(&self.routes.0[index].pattern));

        let declared_length = parts
            .headers
            .get(header::CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok());
        if declared_length.is_some_and(|length| length > self.max_body_bytes as u64) {
            return error_response(StatusCode::PAYLOAD_TOO_LARGE, "request body too large");
        }
        let body = match read_body(body, self.max_body_bytes).await {
            Ok(body) => body,
            Err(BodyError::TooLarge) => {
                return error_response(StatusCode::PAYLOAD_TOO_LARGE, "request body too large");
            }
            Err(BodyError::Read) => {
                return error_response(StatusCode::BAD_REQUEST, "could not read request body");
            }
        };

        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (reply_tx, reply_rx) = oneshot::channel();
        let request = PyHttpRequest {
            id,
            route: index,
            method: parts.method,
            uri: parts.uri,
            headers: parts.headers,
            params,
            body,
            reply: Mutex::new(Some(reply_tx)),
        };
        if self.events.send(request).is_err() {
            return error_response(StatusCode::SERVICE_UNAVAILABLE, "server is stopping");
        }
        // From here until a complete response, losing this future means the
        // client has gone, and Python must hear about it (H-3).
        let mut guard = DisconnectGuard::new(id, self.events.clone());
        let mut abort = self.abort.clone();
        let reply = tokio::select! {
            reply = reply_rx => reply,
            () = aborted(&mut abort) => {
                return error_response(StatusCode::SERVICE_UNAVAILABLE, "server is stopping");
            }
        };
        match reply {
            Ok(Reply::Full {
                status,
                headers,
                body,
            }) => {
                guard.disarm();
                let mut response = Response::new(Body::from(body));
                *response.status_mut() = status;
                *response.headers_mut() = headers;
                response
            }
            Ok(Reply::Stream {
                status,
                headers,
                frames,
                shared,
            }) => {
                let stream = BodyStream {
                    id,
                    frames,
                    shared,
                    events: self.events.clone(),
                    abort,
                    guard,
                };
                let mut response = Response::new(stream.into_body());
                *response.status_mut() = status;
                *response.headers_mut() = headers;
                response
            }
            // Python dropped the request without answering it.
            Err(_) => {
                guard.disarm();
                error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "the handler did not respond",
                )
            }
        }
    }
}

async fn handle(State(state): State<Arc<ServerState>>, request: Request) -> Response {
    let mut record = RequestRecord::new(&state.actor, &request);
    let response = state.dispatch(request, &mut record).await;
    record.status = Some(response.status());
    response
}

/// Serve until told to shut down, then drain (H-4).
async fn run(
    listener: std::net::TcpListener,
    state: Arc<ServerState>,
    shutdown: oneshot::Receiver<Duration>,
    abort: watch::Sender<bool>,
) -> io::Result<()> {
    let listener = tokio::net::TcpListener::from_std(listener)?.tap_io(|stream| {
        // Responses are often small and streamed; do not let Nagle delay them.
        let _ = stream.set_nodelay(true);
    });
    let router = Router::new().fallback(handle).with_state(state);
    let (drain_tx, drain_rx) = oneshot::channel::<()>();
    let mut server = std::pin::pin!(
        axum::serve(listener, router)
            .with_graceful_shutdown(async move {
                let _ = drain_rx.await;
            })
            .into_future()
    );
    let grace = tokio::select! {
        result = &mut server => return result,
        // A dropped sender means the server object is gone: stop at once.
        grace = shutdown => grace.unwrap_or(Duration::ZERO),
    };
    let _ = drain_tx.send(());
    if let Ok(result) = tokio::time::timeout(grace, &mut server).await {
        return result;
    }
    abort.send_replace(true);
    match tokio::time::timeout(ABORT_TIMEOUT, &mut server).await {
        Ok(result) => result,
        Err(_) => {
            tracing::warn!("http server: connections were still open after shutdown");
            Ok(())
        }
    }
}

async fn serve(
    listener: std::net::TcpListener,
    state: Arc<ServerState>,
    shutdown: oneshot::Receiver<Duration>,
    abort: watch::Sender<bool>,
) {
    let events = state.events.clone();
    if let Err(error) = run(listener, state, shutdown, abort).await {
        tracing::error!("http server failed: {error}");
    }
    let _ = events.send(Signal::Closed);
}

/// A running HTTP server. Dropping it stops the server at once.
#[pyclass(
    name = "HttpServer",
    module = "monarch._rust_bindings.monarch_hyperactor.http",
    frozen
)]
pub struct PyHttpServer {
    local_addr: SocketAddr,
    /// Taken by the first call to `shutdown`.
    shutdown: Mutex<Option<oneshot::Sender<Duration>>>,
}

#[pymethods]
impl PyHttpServer {
    /// Bind `host:port` and start serving `routes`, a list of
    /// `(method, path pattern)`. Returns the server and the receiver of its
    /// requests and signals.
    #[staticmethod]
    #[pyo3(signature = (host, port, routes, *, max_body_bytes, actor))]
    fn start(
        host: &str,
        port: u16,
        routes: Vec<(String, String)>,
        max_body_bytes: usize,
        actor: &str,
    ) -> PyResult<(PyHttpServer, PyReceiver)> {
        let routes = Routes::new(&routes).map_err(PyValueError::new_err)?;
        let listener = std::net::TcpListener::bind((host, port))?;
        listener.set_nonblocking(true)?;
        let local_addr = listener.local_addr()?;
        let (events, receiver) = pympsc::channel().map_pyerr()?;
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let (abort_tx, abort_rx) = watch::channel(false);
        let state = Arc::new(ServerState {
            routes,
            events,
            max_body_bytes,
            next_id: AtomicU64::new(0),
            abort: abort_rx,
            actor: actor.into(),
        });
        get_tokio_runtime().spawn(serve(listener, state, shutdown_rx, abort_tx));
        Ok((
            PyHttpServer {
                local_addr,
                shutdown: Mutex::new(Some(shutdown_tx)),
            },
            receiver,
        ))
    }

    /// The address the server is bound to.
    #[getter]
    fn host(&self) -> String {
        self.local_addr.ip().to_string()
    }

    /// The port the server is bound to.
    #[getter]
    fn port(&self) -> u16 {
        self.local_addr.port()
    }

    /// Stop accepting connections and give requests in flight
    /// `grace_seconds` to finish (H-4). Later calls do nothing.
    fn shutdown(&self, grace_seconds: f64) -> PyResult<()> {
        let grace = Duration::try_from_secs_f64(grace_seconds)
            .map_err(|_| PyValueError::new_err(format!("invalid grace period {grace_seconds}")))?;
        let shutdown = self
            .shutdown
            .lock()
            .expect("shutdown lock is never held across a panic")
            .take();
        if let Some(shutdown) = shutdown {
            let _ = shutdown.send(grace);
        }
        Ok(())
    }
}

/// Whether other hosts can be expected to reach `addr`. This is the test
/// `hyperactor::channel::ChannelAddr::any` applies to its `tcp` addresses.
fn is_routable(addr: &IpAddr) -> bool {
    !addr.is_loopback()
        && match addr {
            IpAddr::V6(v6) => !v6.is_unicast_link_local(),
            IpAddr::V4(v4) => !v4.is_link_local(),
        }
}

/// An address of this host that other hosts can use to reach a server bound
/// to all interfaces: the first routable address its host name resolves to,
/// as `ChannelAddr::any` chooses for Monarch's own `tcp` transport.
#[pyfunction]
fn routable_host(py: Python<'_>) -> PyResult<String> {
    let hostname = nix::unistd::gethostname()
        .map_err(io::Error::from)?
        .into_string()
        .map_err(|_| PyRuntimeError::new_err("the host name is not valid UTF-8"))?;
    let addresses = py.detach(|| (hostname.as_str(), 0).to_socket_addrs())?;
    addresses
        .map(|addr| addr.ip())
        .find(is_routable)
        .map(|ip| ip.to_string())
        .ok_or_else(|| {
            PyRuntimeError::new_err(format!(
                "host name {hostname:?} resolves to no address other hosts can reach"
            ))
        })
}

pub fn register_python_bindings(hyperactor_mod: &Bound<'_, PyModule>) -> PyResult<()> {
    hyperactor_mod.add_class::<PyHttpServer>()?;
    hyperactor_mod.add_class::<PyHttpRequest>()?;
    hyperactor_mod.add_class::<PyHttpBodyWriter>()?;
    hyperactor_mod.add("SIGNAL_DISCONNECTED", Signal::DISCONNECTED)?;
    hyperactor_mod.add("SIGNAL_DRAINED", Signal::DRAINED)?;
    hyperactor_mod.add("SIGNAL_CLOSED", Signal::CLOSED)?;
    let routable_host = wrap_pyfunction!(routable_host, hyperactor_mod)?;
    routable_host.setattr(
        "__module__",
        "monarch._rust_bindings.monarch_hyperactor.http",
    )?;
    hyperactor_mod.add_function(routable_host)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn routes(specs: &[(&str, &str)]) -> Routes {
        let specs: Vec<(String, String)> = specs
            .iter()
            .map(|(method, pattern)| ((*method).to_owned(), (*pattern).to_owned()))
            .collect();
        Routes::new(&specs).unwrap()
    }

    fn found(index: usize, params: &[(&str, &str)]) -> Matched {
        Matched::Route {
            index,
            params: params
                .iter()
                .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
                .collect(),
        }
    }

    #[test]
    fn test_pattern_segments() {
        assert_eq!(
            parse_pattern("/v1/models/{model}").unwrap(),
            vec![
                Segment::Literal("v1".to_owned()),
                Segment::Literal("models".to_owned()),
                Segment::Param("model".to_owned()),
            ]
        );
        assert_eq!(
            parse_pattern("/").unwrap(),
            vec![Segment::Literal(String::new())]
        );
    }

    #[test]
    fn test_pattern_rejects_malformed_paths() {
        for pattern in [
            "", "v1", "/a?b", "/a#b", "/a b", "/{}", "/{a-b}", "/x{a}", "/{a}x", "/{a}/{a}",
        ] {
            assert!(parse_pattern(pattern).is_err(), "{pattern:?} was accepted");
        }
    }

    #[test]
    fn test_routes_reject_duplicates_and_bad_methods() {
        let duplicate = [
            ("GET".to_owned(), "/a/{x}".to_owned()),
            ("GET".to_owned(), "/a/{y}".to_owned()),
        ];
        assert!(Routes::new(&duplicate).is_err());
        let bad_method = [("GE T".to_owned(), "/a".to_owned())];
        assert!(Routes::new(&bad_method).is_err());
        // The same path under two methods is two routes.
        routes(&[("GET", "/a"), ("POST", "/a")]);
    }

    #[test]
    fn test_find_matches_method_and_path() {
        let routes = routes(&[
            ("GET", "/healthz"),
            ("POST", "/v1/generate"),
            ("GET", "/v1/models/{model}"),
        ]);
        assert_eq!(routes.find(&Method::GET, "/healthz"), found(0, &[]));
        assert_eq!(routes.find(&Method::POST, "/v1/generate"), found(1, &[]));
        assert_eq!(
            routes.find(&Method::GET, "/v1/models/a%2Fb"),
            found(2, &[("model", "a%2Fb")])
        );
        assert_eq!(routes.find(&Method::GET, "/missing"), Matched::NotFound);
        assert_eq!(routes.find(&Method::GET, "/healthz/"), Matched::NotFound);
        assert_eq!(routes.find(&Method::GET, "/v1/models/"), Matched::NotFound);
        assert_eq!(routes.find(&Method::GET, "*"), Matched::NotFound);
    }

    #[test]
    fn test_find_reports_allowed_methods() {
        let routes = routes(&[("GET", "/a"), ("POST", "/a"), ("PUT", "/b")]);
        assert_eq!(
            routes.find(&Method::DELETE, "/a"),
            Matched::MethodNotAllowed("GET, POST".to_owned())
        );
    }

    #[test]
    fn test_find_prefers_literal_segments() {
        let routes = routes(&[("GET", "/models/{model}"), ("GET", "/models/default")]);
        assert_eq!(routes.find(&Method::GET, "/models/default"), found(1, &[]));
        assert_eq!(
            routes.find(&Method::GET, "/models/other"),
            found(0, &[("model", "other")])
        );
    }

    #[test]
    fn test_head_falls_back_to_get() {
        let get_only = routes(&[("GET", "/a")]);
        assert_eq!(get_only.find(&Method::HEAD, "/a"), found(0, &[]));
        let both = routes(&[("GET", "/a"), ("HEAD", "/a")]);
        assert_eq!(both.find(&Method::HEAD, "/a"), found(1, &[]));
    }

    #[test]
    fn test_stream_writer_waits_above_high_water_until_low_water() {
        let shared = StreamShared::default();
        assert!(!shared.wrote(STREAM_HIGH_WATER));
        // One more byte crosses the mark: the writer must wait.
        assert!(shared.wrote(1));
        // Still above the low-water mark: no wake-up yet.
        assert!(!shared.took(STREAM_HIGH_WATER - STREAM_LOW_WATER));
        // At the low-water mark the waiting writer is woken, once.
        assert!(shared.took(1));
        assert!(!shared.took(STREAM_LOW_WATER));
        assert_eq!(shared.queued.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn test_stream_writer_is_not_woken_unless_waiting() {
        let shared = StreamShared::default();
        assert!(!shared.wrote(10));
        assert!(!shared.took(10));
    }

    #[test]
    fn test_is_routable() {
        let routable = |addr: &str| is_routable(&addr.parse().unwrap());
        assert!(routable("10.0.0.5"));
        assert!(routable("2001:db8::1"));
        assert!(!routable("127.0.0.1"));
        assert!(!routable("::1"));
        assert!(!routable("169.254.1.1"));
        assert!(!routable("fe80::1"));
    }
}
