/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::ffi::OsStr;
use std::future::poll_fn;
use std::os::unix::ffi::OsStrExt as _;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

use tokio::io::AsyncBufReadExt as _;
use tokio::io::AsyncReadExt as _;
use tokio::io::AsyncWriteExt as _;
use tokio::io::BufReader;
use tokio::net::UnixListener;
use tokio::net::UnixStream;
use tokio::time::timeout;

use super::engine::QUEUE_CAPACITY;
use super::engine::TaskCancellation;
use super::native::native_connect;
use super::*;
use crate::DatagramAddr;
use crate::DatagramSocket;
use crate::DatagramSocketSet;
use crate::UdpSocket;

const WAIT: Duration = Duration::from_secs(5);
/// The CID of the local host's vsock loopback transport.
const VMADDR_CID_LOCAL: u32 = 1;

static NEXT_PATH: AtomicU64 = AtomicU64::new(1);

fn socket_path(label: &str) -> PathBuf {
    let id = NEXT_PATH.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "chrysalis-vsock-{label}-{}-{id}.sock",
        std::process::id()
    ))
}

fn node(name: &str) -> DatagramAddr {
    DatagramAddr::try_from(&VsockDatagramAddr::Node(name.to_owned())).expect("node address")
}

fn unix(path: &Path) -> DatagramAddr {
    DatagramAddr::try_from(&VsockDatagramAddr::Unix(path.to_owned())).expect("unix address")
}

fn hybrid(port: u32, uds: &Path) -> DatagramAddr {
    DatagramAddr::try_from(&VsockDatagramAddr::Hybrid {
        port,
        uds: uds.to_owned(),
    })
    .expect("hybrid address")
}

async fn recv(socket: &VsockDatagramSocket) -> (Vec<u8>, DatagramAddr) {
    let mut buffer = vec![0; 70_000];
    let (len, source) = timeout(WAIT, socket.recv_from(&mut buffer))
        .await
        .expect("receive timed out")
        .expect("receive failed");
    buffer.truncate(len);
    (buffer, source)
}

/// Sends once the socket reports send readiness, as a UDP socket requires.
async fn send_when_ready<S>(socket: &S, bytes: &[u8], destination: &DatagramAddr)
where
    S: DatagramSocket + ?Sized,
{
    let transmit = DatagramTransmit {
        destination,
        contents: bytes,
        segment_size: None,
        ecn: None,
        source_ip: None,
    };
    poll_fn(|cx| socket.poll_send_ready(cx, &transmit))
        .await
        .expect("wait for send readiness");
    socket.try_send_to(bytes, destination).expect("send");
}

async fn write_frame(stream: &mut UnixStream, kind: u8, payload: &[u8]) {
    stream.write_u8(kind).await.expect("write frame kind");
    stream
        .write_u32(payload.len() as u32)
        .await
        .expect("write frame length");
    stream
        .write_all(payload)
        .await
        .expect("write frame payload");
}

/// Sends `probe` to `name` from `socket` until a frame reaches `client`,
/// and returns that frame.
async fn probe_until_delivered(
    socket: &VsockDatagramSocket,
    name: &str,
    client: &mut UnixStream,
) -> (u8, Vec<u8>) {
    timeout(WAIT, async {
        loop {
            let _ = socket.try_send_to(b"probe", &node(name));
            if let Ok(Ok(kind)) = timeout(Duration::from_millis(50), client.read_u8()).await {
                let len = client.read_u32().await.expect("read frame length");
                let mut payload = vec![0; len as usize];
                client
                    .read_exact(&mut payload)
                    .await
                    .expect("read frame payload");
                return (kind, payload);
            }
        }
    })
    .await
    .expect("a send should reach the client")
}

async fn read_frame(stream: &mut UnixStream) -> (u8, Vec<u8>) {
    timeout(WAIT, async {
        let kind = stream.read_u8().await.expect("read frame kind");
        let len = stream.read_u32().await.expect("read frame length");
        let mut payload = vec![0; len as usize];
        stream
            .read_exact(&mut payload)
            .await
            .expect("read frame payload");
        (kind, payload)
    })
    .await
    .expect("frame read timed out")
}

/// Waits until the peer closes `stream`: a read returns end of file, or
/// the connection is reset because unread bytes were discarded.
async fn assert_closed(stream: &mut UnixStream) {
    let mut byte = [0; 1];
    loop {
        match timeout(WAIT, stream.read(&mut byte))
            .await
            .expect("waiting for the connection to close timed out")
        {
            Ok(0) => return,
            Err(error) if error.kind() == io::ErrorKind::ConnectionReset => return,
            Ok(_) => continue,
            Err(error) => panic!("unexpected read error: {error}"),
        }
    }
}

/// A fake Firecracker hybrid-vsock socket. Each connection reads one line,
/// reports it, and replies with the next scripted answer; after an `OK`
/// reply it relays the connection to `target`. With no `target`, it holds
/// the connection open without relaying.
struct FakeVmm {
    path: PathBuf,
    lines: tokio::sync::mpsc::UnboundedReceiver<String>,
}

fn fake_vmm(replies: Vec<Option<&'static str>>, target: Option<PathBuf>) -> FakeVmm {
    let path = socket_path("vmm");
    let listener = UnixListener::bind(&path).expect("bind fake VMM");
    let (lines_tx, lines) = tokio::sync::mpsc::unbounded_channel();
    tokio::spawn(async move {
        let mut replies = replies.into_iter();
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            let reply = replies.next().flatten();
            let target = target.clone();
            let lines_tx = lines_tx.clone();
            tokio::spawn(async move {
                let mut stream = BufReader::new(stream);
                let mut line = String::new();
                if stream.read_line(&mut line).await.is_err() {
                    return;
                }
                let _ = lines_tx.send(line);
                let Some(reply) = reply else {
                    // Hold the connection open, never replying.
                    let mut rest = Vec::new();
                    let _ = stream.read_to_end(&mut rest).await;
                    return;
                };
                let mut stream = stream.into_inner();
                if stream.write_all(reply.as_bytes()).await.is_err() {
                    return;
                }
                if !reply.starts_with("OK") {
                    return;
                }
                let Some(target) = target else {
                    return;
                };
                let Ok(mut upstream) = UnixStream::connect(&target).await else {
                    return;
                };
                let _ = tokio::io::copy_bidirectional(&mut stream, &mut upstream).await;
            });
        }
    });
    FakeVmm { path, lines }
}

#[tokio::test]
async fn addresses_round_trip() {
    for address in [
        VsockDatagramAddr::Node("a".to_owned()),
        VsockDatagramAddr::Native {
            cid: VMADDR_CID_HOST,
            port: 5001,
        },
        VsockDatagramAddr::Native {
            cid: VMADDR_CID_ANY,
            port: 5000,
        },
        VsockDatagramAddr::Hybrid {
            port: 5000,
            uds: PathBuf::from("/tmp/a:b/v.sock"),
        },
        VsockDatagramAddr::Unix(PathBuf::from("/tmp/v.sock_5001")),
    ] {
        let addr = DatagramAddr::try_from(&address).expect("encode address");
        assert_eq!(addr.scheme(), VSOCK_SCHEME);
        assert_eq!(VsockDatagramAddr::try_from(&addr).expect("decode"), address);
    }

    for opaque in [
        &b"native:x:1"[..],
        b"native:1",
        b"hybrid:1:",
        b"unix:",
        b"node:",
        b"bogus:1",
        b"\xff",
    ] {
        let error = VsockDatagramAddr::try_from(&DatagramAddr::new(VSOCK_SCHEME, opaque))
            .expect_err("malformed address must be rejected");
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput, "{opaque:?}");
    }
    assert_eq!(
        VsockDatagramAddr::try_from(&DatagramAddr::new("udp", &b"node:a"[..]))
            .expect_err("another scheme must be rejected")
            .kind(),
        io::ErrorKind::InvalidInput
    );

    let non_utf8 = PathBuf::from(OsStr::from_bytes(b"/tmp/\xff"));
    assert_eq!(
        DatagramAddr::try_from(&VsockDatagramAddr::Unix(non_utf8))
            .expect_err("a non-UTF-8 path has no address")
            .kind(),
        io::ErrorKind::InvalidInput
    );
    assert_eq!(
        DatagramAddr::try_from(&VsockDatagramAddr::Node(String::new()))
            .expect_err("an empty name has no address")
            .kind(),
        io::ErrorKind::InvalidInput
    );
    assert_eq!(
        VsockDatagramSocket::bind("", &[])
            .expect_err("an empty name cannot be bound")
            .kind(),
        io::ErrorKind::InvalidInput
    );

    let longest = "n".repeat(65_530);
    VsockDatagramSocket::bind(&longest, &[]).expect("a 65,530-byte name fits in a HELLO");
    assert_eq!(
        VsockDatagramSocket::bind(&format!("{longest}n"), &[])
            .expect_err("a 65,531-byte name does not fit in a HELLO")
            .kind(),
        io::ErrorKind::InvalidInput
    );
}

#[tokio::test]
async fn unix_dial_reply_uses_symmetric_labels() {
    let path = socket_path("a");
    let a =
        VsockDatagramSocket::bind("a", &[VsockDatagramAddr::Unix(path.clone())]).expect("bind a");
    let b = VsockDatagramSocket::bind("b", &[]).expect("bind b");

    for round in 0..2 {
        b.try_send_to(b"ping", &unix(&path)).expect("b sends ping");
        assert_eq!(
            recv(&a).await,
            (b"ping".to_vec(), node("b")),
            "round {round}"
        );
        a.try_send_to(b"pong", &node("b")).expect("a replies");
        assert_eq!(
            recv(&b).await,
            (b"pong".to_vec(), unix(&path)),
            "round {round}"
        );
    }
    assert_eq!(a.inner.accepted.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn hybrid_dial_speaks_connect_handshake() {
    let path = socket_path("a");
    let a =
        VsockDatagramSocket::bind("a", &[VsockDatagramAddr::Unix(path.clone())]).expect("bind a");
    let mut vmm = fake_vmm(vec![Some("OK 1\n")], Some(path.clone()));
    let b = VsockDatagramSocket::bind("b", &[]).expect("bind b");

    b.try_send_to(b"ping", &hybrid(5000, &vmm.path))
        .expect("b sends ping");
    assert_eq!(recv(&a).await, (b"ping".to_vec(), node("b")));
    assert_eq!(vmm.lines.recv().await.as_deref(), Some("CONNECT 5000\n"));
    a.try_send_to(b"pong", &node("b")).expect("a replies");
    assert_eq!(recv(&b).await, (b"pong".to_vec(), hybrid(5000, &vmm.path)));
}

#[tokio::test]
async fn refused_hybrid_dial_redials_on_next_send() {
    let path = socket_path("a");
    let a =
        VsockDatagramSocket::bind("a", &[VsockDatagramAddr::Unix(path.clone())]).expect("bind a");
    let mut vmm = fake_vmm(vec![Some("NO\n"), Some("OK 1\n")], Some(path.clone()));
    let b = VsockDatagramSocket::bind("b", &[]).expect("bind b");
    let destination = hybrid(5000, &vmm.path);

    b.try_send_to(b"dropped", &destination)
        .expect("the first send is queued");
    assert_eq!(vmm.lines.recv().await.as_deref(), Some("CONNECT 5000\n"));
    // The refused dial removes its entry; a later send dials again.
    timeout(WAIT, async {
        while b
            .inner
            .connections
            .lock()
            .unwrap()
            .contains_key(&destination)
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the refused dial should remove its entry");
    b.try_send_to(b"delivered", &destination)
        .expect("the second send is queued");
    assert_eq!(recv(&a).await, (b"delivered".to_vec(), node("b")));
}

#[tokio::test]
async fn send_to_unconnected_node_is_host_unreachable() {
    let b = VsockDatagramSocket::bind("b", &[]).expect("bind b");
    assert_eq!(
        b.try_send_to(b"x", &node("nobody"))
            .expect_err("no connection announced that name")
            .kind(),
        io::ErrorKind::HostUnreachable
    );
    for destination in [
        DatagramAddr::new("udp", &[4, 127, 0, 0, 1, 0, 1][..]),
        DatagramAddr::new(VSOCK_SCHEME, &b"native:x:1"[..]),
    ] {
        assert_eq!(
            b.try_send_to(b"x", &destination)
                .expect_err("not a vsock address")
                .kind(),
            io::ErrorKind::AddrNotAvailable
        );
    }
    assert_eq!(
        b.try_send_to(&vec![0; 65_536], &node("nobody"))
            .expect_err("oversized datagram")
            .kind(),
        io::ErrorKind::InvalidInput
    );
}

#[tokio::test]
async fn malformed_frames_close_only_that_connection() {
    let path = socket_path("a");
    let a =
        VsockDatagramSocket::bind("a", &[VsockDatagramAddr::Unix(path.clone())]).expect("bind a");

    let mut datagram_first = UnixStream::connect(&path).await.expect("connect");
    write_frame(&mut datagram_first, 1, b"no hello").await;
    assert_closed(&mut datagram_first).await;

    let mut oversized = UnixStream::connect(&path).await.expect("connect");
    write_frame(&mut oversized, 0, b"node:oversized").await;
    oversized.write_u8(1).await.expect("write kind");
    oversized.write_u32(65_536).await.expect("write length");
    assert_closed(&mut oversized).await;

    let mut unknown = UnixStream::connect(&path).await.expect("connect");
    write_frame(&mut unknown, 0, b"node:unknown").await;
    write_frame(&mut unknown, 7, b"?").await;
    assert_closed(&mut unknown).await;

    let b = VsockDatagramSocket::bind("b", &[]).expect("bind b");
    b.try_send_to(b"ping", &unix(&path)).expect("b sends ping");
    assert_eq!(recv(&a).await, (b"ping".to_vec(), node("b")));
}

#[tokio::test]
async fn newer_connection_with_same_name_replaces_older() {
    let path = socket_path("a");
    let a =
        VsockDatagramSocket::bind("a", &[VsockDatagramAddr::Unix(path.clone())]).expect("bind a");
    let mut first = UnixStream::connect(&path).await.expect("connect first");
    write_frame(&mut first, 0, b"node:x").await;
    // The first incarnation holds node:x before the second connects.
    assert_eq!(probe_until_delivered(&a, "x", &mut first).await.0, 1);
    let mut second = UnixStream::connect(&path).await.expect("connect second");
    write_frame(&mut second, 0, b"node:x").await;

    // 1. The second incarnation holds the entry once a send reaches it.
    let (kind, payload) = probe_until_delivered(&a, "x", &mut second).await;
    assert_eq!((kind, payload.as_slice()), (1, &b"probe"[..]));
    // 2. The first connection was closed when it was replaced.
    assert_closed(&mut first).await;
    // 3. Anything the first client writes now is never delivered.
    let _ = first.write_all(&[1, 0, 0, 0, 3, b'o', b'l', b'd']).await;
    // 4. The next datagram `a` receives is the second client's marker.
    write_frame(&mut second, 1, b"marker").await;
    loop {
        let (bytes, source) = recv(&a).await;
        assert_eq!(source, node("x"));
        assert_ne!(
            bytes, b"old",
            "the replaced connection delivered a datagram"
        );
        if bytes == b"marker" {
            break;
        }
    }
    // 5. Dropping the first client leaves node:x routed to the second.
    drop(first);
    a.try_send_to(b"still", &node("x")).expect("send to node:x");
    loop {
        let (kind, payload) = read_frame(&mut second).await;
        assert_eq!(kind, 1);
        if payload == b"still" {
            break;
        }
    }
}

#[tokio::test]
async fn shutdown_releases_every_task() {
    let path = socket_path("a");
    let a =
        VsockDatagramSocket::bind("a", &[VsockDatagramAddr::Unix(path.clone())]).expect("bind a");
    let b = VsockDatagramSocket::bind("b", &[]).expect("bind b");

    // Fill a's receive queue until one of its readers is blocked on it.
    let backpressure = a.inner.backpressure.clone();
    let blocked = backpressure.notified();
    tokio::pin!(blocked);
    timeout(WAIT, async {
        loop {
            tokio::select! {
                () = &mut blocked => return,
                () = tokio::task::yield_now() => {
                    let _ = b.try_send_to(b"fill", &unix(&path));
                }
            }
        }
    })
    .await
    .expect("a's receive queue should fill");

    // Leave a hybrid dial waiting on a VMM that never replies.
    let mut vmm = fake_vmm(vec![None], None);
    a.try_send_to(b"pending", &hybrid(5000, &vmm.path))
        .expect("the dial is queued");
    assert_eq!(
        timeout(WAIT, vmm.lines.recv())
            .await
            .expect("VMM read")
            .as_deref(),
        Some("CONNECT 5000\n")
    );

    a.shutdown();
    timeout(WAIT, a.join())
        .await
        .expect("shutdown should release every task");
    assert_eq!(
        a.try_send_to(b"after", &node("b"))
            .expect_err("send after shutdown")
            .kind(),
        io::ErrorKind::BrokenPipe
    );
    let mut buffer = vec![0; 16];
    for _ in 0..=QUEUE_CAPACITY {
        match timeout(WAIT, a.recv_from(&mut buffer))
            .await
            .expect("receive after shutdown must not block")
        {
            Ok(_) => continue,
            Err(error) => {
                assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);
                return;
            }
        }
    }
    panic!("receives should fail once the queued datagrams are drained");
}

/// Records how many tasks were still counted when it was dropped.
struct CountOnDrop {
    engine: Arc<VsockDatagramEngine>,
    seen: Arc<AtomicUsize>,
}

impl Drop for CountOnDrop {
    fn drop(&mut self) {
        self.seen
            .store(self.engine.tasks.load(Ordering::Acquire), Ordering::Release);
    }
}

#[tokio::test]
async fn task_resources_are_released_before_join_completes() {
    // A task refused because the socket already shut down.
    let refused = VsockDatagramSocket::bind("refused", &[]).expect("bind");
    refused.shutdown();
    let seen = Arc::new(AtomicUsize::new(usize::MAX));
    let witness = CountOnDrop {
        engine: refused.inner.clone(),
        seen: seen.clone(),
    };
    refused
        .inner
        .spawn(TaskCancellation::ShutdownOnly, async move {
            let _witness = witness;
        });
    timeout(WAIT, refused.join()).await.expect("join");
    assert_eq!(
        seen.load(Ordering::Acquire),
        1,
        "a refused task's future must be dropped while it is still counted"
    );

    // A running task cancelled by shutdown.
    let running = VsockDatagramSocket::bind("running", &[]).expect("bind");
    let seen = Arc::new(AtomicUsize::new(usize::MAX));
    let witness = CountOnDrop {
        engine: running.inner.clone(),
        seen: seen.clone(),
    };
    running
        .inner
        .spawn(TaskCancellation::ShutdownOnly, async move {
            let _witness = witness;
            std::future::pending::<()>().await;
        });
    tokio::task::yield_now().await;
    running.shutdown();
    timeout(WAIT, running.join()).await.expect("join");
    assert_eq!(
        seen.load(Ordering::Acquire),
        1,
        "a cancelled task's future must be dropped while it is still counted"
    );
}

#[tokio::test]
async fn dropping_the_socket_releases_its_tasks() {
    let path = socket_path("a");
    let a =
        VsockDatagramSocket::bind("a", &[VsockDatagramAddr::Unix(path.clone())]).expect("bind a");
    let mut client = UnixStream::connect(&path).await.expect("connect");
    write_frame(&mut client, 0, b"node:client").await;
    // The client is registered once a send to its name reaches it.
    assert_eq!(probe_until_delivered(&a, "client", &mut client).await.0, 1);

    drop(a);
    assert_closed(&mut client).await;
    let error = UnixStream::connect(&path)
        .await
        .expect_err("the listener should be gone");
    assert!(
        matches!(
            error.kind(),
            io::ErrorKind::NotFound | io::ErrorKind::ConnectionRefused
        ),
        "{error}"
    );
}

#[tokio::test]
async fn composes_with_udp_in_a_socket_set() {
    let vsock = Arc::new(VsockDatagramSocket::bind("set", &[]).expect("bind vsock"));
    let udp = Arc::new(
        UdpSocket::bind((std::net::Ipv4Addr::LOCALHOST, 0).into())
            .await
            .expect("bind UDP"),
    );
    let set = DatagramSocketSet::new(vsock.clone(), vec![udp.clone() as Arc<dyn DatagramSocket>])
        .expect("socket set");
    let udp_peer = UdpSocket::bind((std::net::Ipv4Addr::LOCALHOST, 0).into())
        .await
        .expect("bind UDP peer");
    let peer_path = socket_path("peer");
    let vsock_peer =
        VsockDatagramSocket::bind("peer", &[VsockDatagramAddr::Unix(peer_path.clone())])
            .expect("bind vsock peer");

    send_when_ready(&set, b"to udp", udp_peer.local_addr()).await;
    let mut buffer = [0; 64];
    let (len, source) = timeout(WAIT, udp_peer.recv_from(&mut buffer))
        .await
        .expect("UDP receive timed out")
        .expect("UDP receive");
    assert_eq!(
        (&buffer[..len], &source),
        (&b"to udp"[..], udp.local_addr())
    );

    set.try_send_to(b"to vsock", &unix(&peer_path))
        .expect("send to vsock peer");
    assert_eq!(recv(&vsock_peer).await, (b"to vsock".to_vec(), node("set")));

    send_when_ready(&udp_peer, b"from udp", udp.local_addr()).await;
    vsock_peer
        .try_send_to(b"from vsock", &node("set"))
        .expect("vsock reply");
    let mut seen = Vec::new();
    for _ in 0..2 {
        let (len, source) = timeout(WAIT, set.recv_from(&mut buffer))
            .await
            .expect("set receive timed out")
            .expect("set receive");
        seen.push((buffer[..len].to_vec(), source));
    }
    seen.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(
        seen,
        vec![
            (b"from udp".to_vec(), udp_peer.local_addr().clone()),
            (b"from vsock".to_vec(), unix(&peer_path)),
        ]
    );
}

/// Whether `error` means this host has no vsock support or no loopback
/// transport, rather than a failure of the carrier.
fn vsock_unavailable(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::AddrNotAvailable | io::ErrorKind::NotFound
    ) || matches!(
        error.raw_os_error(),
        Some(libc::EAFNOSUPPORT | libc::ENODEV)
    )
}

#[tokio::test]
async fn native_loopback_round_trips() {
    // Try a few ports derived from the process ID, skipping any in use.
    let first_port = 40_000 + std::process::id() % 10_000;
    let mut bound = None;
    for port in first_port..first_port + 16 {
        match VsockDatagramSocket::bind(
            "a",
            &[VsockDatagramAddr::Native {
                cid: VMADDR_CID_ANY,
                port,
            }],
        ) {
            Ok(a) => {
                bound = Some((a, port));
                break;
            }
            Err(error) if error.kind() == io::ErrorKind::AddrInUse => continue,
            Err(error) if vsock_unavailable(&error) => {
                eprintln!("skipping: this host has no vsock support: {error}");
                return;
            }
            Err(error) => panic!("binding a vsock listener failed: {error}"),
        }
    }
    let (a, port) = bound.expect("one of 16 vsock ports should be free");
    match timeout(WAIT, native_connect(VMADDR_CID_LOCAL, port))
        .await
        .expect("connecting over vsock loopback timed out")
    {
        Ok(_) => {}
        Err(error) if vsock_unavailable(&error) => {
            eprintln!("skipping: this host has no vsock loopback: {error}");
            return;
        }
        Err(error) => panic!("connecting over vsock loopback failed: {error}"),
    }

    let b = VsockDatagramSocket::bind("b", &[]).expect("bind b");
    let destination = DatagramAddr::try_from(&VsockDatagramAddr::Native {
        cid: VMADDR_CID_LOCAL,
        port,
    })
    .expect("native address");
    b.try_send_to(b"ping", &destination).expect("b sends ping");
    assert_eq!(recv(&a).await, (b"ping".to_vec(), node("b")));
    a.try_send_to(b"pong", &node("b")).expect("a replies");
    assert_eq!(recv(&b).await, (b"pong".to_vec(), destination));
}
