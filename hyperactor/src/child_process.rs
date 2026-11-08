/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Spawns child processes from a thread that lives as long as the process.
//!
//! On Linux, `prctl(PR_SET_PDEATHSIG)` fires when the *thread* that forked a
//! child exits, not when the child's parent process exits. A child forked
//! from a tokio worker therefore dies with that worker, and tokio can retire a
//! worker at any time, for example after `block_in_place`. Children that arm
//! the death signal must be forked here instead, by one thread that never
//! exits, so the signal fires only when the parent process does.
//!
//! # Invariants
//!
//! - **CPS-1 (spawner-outlives-children):** every child returned by
//!   [`spawn_child`] is forked by the child-spawner thread, which does not
//!   exit while the process runs. Its request sender is never dropped, and a
//!   panic while spawning a command or dropping it is contained inside the
//!   thread's loop.
//! - **CPS-2 (caller-runtime):** the returned [`Child`] is registered with the
//!   runtime that called [`spawn_child`]: the caller's handle is taken before
//!   the request is sent and entered on the spawner thread for the spawn.
//! - **CPS-3 (no-silent-fallback):** [`spawn_child`] never forks on the
//!   calling thread; it has no path that calls [`Command::spawn`] there.
//! - **CPS-4 (caller-owns-child):** the child returned by a successful
//!   [`spawn_child`] belongs to its caller from the moment the call returns:
//!   the handoff is synchronous, so there is no cancellation point between the
//!   fork and the return. A spawn that panics after forking, as tokio does for
//!   a runtime without I/O, returns an error and leaves that child
//!   unregistered.

use std::any::Any;
use std::io;
use std::mem;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::sync::OnceLock;
use std::sync::mpsc;
use std::thread;

use tokio::process::Child;
use tokio::process::Command;
use tokio::runtime::Handle;

/// Name of the thread that forks every child spawned through [`spawn_child`].
/// Linux keeps the first 15 bytes of a thread name.
const SPAWNER_THREAD_NAME: &str = "child-spawner";

/// A caller's spawn request to the child-spawner thread.
struct Request {
    /// The command to spawn on that thread, rather than on the caller's thread.
    command: Command,

    /// The caller's tokio runtime. The spawner enters it before spawning so
    /// the child is registered with this runtime, not a runtime of its own.
    runtime: Handle,

    /// Sends the child or spawn error back to the caller that made this request.
    reply: mpsc::Sender<io::Result<Child>>,
}

/// Request sender, initialized when the child-spawner thread starts. A failed
/// start leaves it unset, so later callers can retry. Once stored, the sender
/// is never dropped, keeping the thread's request loop alive.
static SPAWNER: OnceLock<mpsc::Sender<Request>> = OnceLock::new();

/// Spawns `command` on a thread that stays alive until this process exits.
/// The returned child is registered with the caller's tokio runtime.
///
/// Use this when the child will call `prctl(PR_SET_PDEATHSIG, SIGKILL)`.
/// Linux then kills the child when the thread that forked it exits, even
/// if the parent process is still running. A tokio worker can be retired;
/// the child-spawner thread stays alive, so retiring a worker cannot kill
/// the child.
///
/// This helper does not set the death signal; the child sets it itself.
///
/// Blocks the caller until spawning succeeds or fails. Requests from all
/// callers are handled one at a time.
///
/// Returns an error if the caller has no current tokio runtime, the
/// child-spawner thread cannot be started, or spawning fails or panics.
pub fn spawn_child(command: Command) -> io::Result<Child> {
    // CPS-2: register the child with the caller's runtime, not the spawner's.
    let runtime = Handle::try_current().map_err(io::Error::other)?;
    let (reply, result) = mpsc::channel();
    // CPS-3: on failure, return an error; never spawn on the caller's thread.
    spawner()?
        .send(Request {
            command,
            runtime,
            reply,
        })
        .map_err(|_| io::Error::other("the child-spawner thread has exited"))?;
    // CPS-4: if we awaited the reply, the caller's task could be cancelled
    // after the child starts but before it receives the handle needed to
    // wait for or stop that child. Wait synchronously so cancellation cannot
    // interrupt this handoff.
    result
        .recv()
        .map_err(|_| io::Error::other("the child-spawner thread dropped the request"))?
}

/// Returns the request sender for the OS thread used by [`spawn_child`].
///
/// On first use, creates a channel and starts one OS thread running
/// [`serve`] to receive the requests and spawn children. [`SPAWNER`] keeps
/// the sender, so later calls use the same thread rather than start another.
///
/// If starting the thread fails, returns the error and leaves [`SPAWNER`]
/// uninitialized, allowing a later call to try again.
fn spawner() -> io::Result<&'static mpsc::Sender<Request>> {
    SPAWNER.get_or_try_init(|| {
        let (sender, requests) = mpsc::channel();
        thread::Builder::new()
            .name(SPAWNER_THREAD_NAME.to_string())
            .spawn(move || serve(requests))?;
        Ok(sender)
    })
}

/// Runs the request loop on the child-spawner OS thread.
///
/// Waits for a request, enters the caller's tokio runtime, spawns the
/// child, and sends the result back to that caller. Requests are handled
/// one at a time.
///
/// [`SPAWNER`] keeps the channel open for the process's lifetime. Panics
/// while spawning or dropping a command are caught so this thread keeps
/// running and children spawned earlier do not lose their parent thread.
fn serve(requests: mpsc::Receiver<Request>) {
    // CPS-1: `SPAWNER` keeps the channel open for the process's lifetime.
    // When there are no requests, this OS thread blocks waiting for the
    // next one rather than exiting.
    for Request {
        mut command,
        runtime,
        reply,
    } in requests
    {
        // CPS-1: a panic must not end this thread and trigger the parent-death
        // signals of children it spawned earlier. Catch it and return an error
        // to this caller instead. For example, tokio can start the child and then
        // panic because the caller's runtime has I/O disabled.
        let result = catch_unwind(AssertUnwindSafe(|| {
            // CPS-2: make the caller's tokio runtime current on this OS thread
            // so `spawn` registers the child with it. The `_runtime` guard
            // restores the previous runtime context when this block ends,
            // including on panic.
            let _runtime = runtime.enter();
            command.spawn()
        }))
        .unwrap_or_else(|panic| {
            // CPS-1: copy the panic's description into the error, then forget
            // its payload without running its destructor. That destructor could
            // panic again, ending this thread and killing earlier children.
            Err(io::Error::other(describe_and_forget(
                panic,
                "spawning the child",
            )))
        });
        // CPS-1: the command owns caller state, such as a `pre_exec` closure's
        // captures, whose destructor could panic. Drop it inside the unwind
        // boundary too, and forget any payload unread, since its destructor is
        // caller code. Nothing is logged here: a `tracing` subscriber is caller
        // code as well. CPS-4: a child that was spawned is still handed over.
        if let Err(panic) = catch_unwind(AssertUnwindSafe(move || drop(command))) {
            mem::forget(panic);
        }
        // A caller that has gone away no longer wants the result.
        let _ = reply.send(result);
    }
}

/// Describes a caught panic, then forgets its payload. CPS-1: the payload can
/// be caller code whose destructor panics again, which would unwind this
/// thread, so its destructor must never run here.
fn describe_and_forget(panic: Box<dyn Any + Send>, what: &str) -> String {
    let description = format!("{what} panicked: {}", panic_message(panic.as_ref()));
    mem::forget(panic);
    description
}

/// Borrows the message from a `&str` or `String` panic payload.
/// For other payloads, returns a fixed description without invoking
/// caller-supplied formatting code.
fn panic_message(panic: &(dyn Any + Send)) -> &str {
    if let Some(message) = panic.downcast_ref::<&str>() {
        message
    } else if let Some(message) = panic.downcast_ref::<String>() {
        message
    } else {
        "a non-string panic payload"
    }
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use std::fs;
    use std::fs::File;
    use std::io::Read;
    use std::os::fd::AsRawFd;
    use std::os::fd::FromRawFd;
    use std::os::fd::OwnedFd;
    use std::os::unix::process::ExitStatusExt;
    use std::panic::panic_any;
    use std::process::Stdio;
    use std::ptr;
    use std::time::Duration;
    use std::time::Instant;

    use nix::unistd::gettid;
    use tokio::runtime::Runtime;

    use super::*;

    /// How long a child must keep running to count as having survived.
    const SURVIVAL_WAIT: Duration = Duration::from_secs(1);
    /// Upper bound on any wait for a child to exit.
    const EXIT_WAIT: Duration = Duration::from_secs(10);

    /// Builds a command to run `sleep <seconds>` with a parent-death signal.
    ///
    /// `pre_exec` runs in the child before starting `sleep` and asks Linux to
    /// send SIGKILL when the forking thread exits. `kill_on_drop` is separate:
    /// it kills the child if a failing test drops its `Child` handle. Tests
    /// keep that handle alive while checking the parent-death signal.
    fn sleep_with_death_signal(seconds: &str) -> Command {
        let mut command = Command::new("sleep");
        command.arg(seconds).kill_on_drop(true);
        // SAFETY: the closure runs in the forked child before exec, where only
        // async-signal-safe calls are allowed. It calls `prctl`, which is, with
        // constant arguments, and `last_os_error`, which reads `errno` and does
        // not allocate.
        unsafe {
            command.pre_exec(|| {
                if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL) != 0 {
                    return Err(io::Error::last_os_error());
                }
                Ok(())
            });
        }
        command
    }

    /// Requests a child from a temporary OS thread and joins that thread
    /// before returning the child handle to the test.
    ///
    /// `spawn` selects either [`spawn_child`] or ordinary `Command::spawn`,
    /// allowing both paths to use the same setup. The temporary thread enters
    /// the test's runtime so either path can register the child with tokio.
    fn spawn_from_short_lived_thread(
        command: Command,
        spawn: fn(Command) -> io::Result<Child>,
    ) -> Child {
        let runtime = Handle::current();
        thread::spawn(move || {
            // A new OS thread has no current tokio runtime of its own.
            // Use the test's runtime while making the spawn request.
            let _runtime = runtime.enter();
            spawn(command)
        })
        .join()
        .expect("the spawning thread panicked")
        .expect("spawn the child")
    }

    /// Watches `child` for `wait`, returning false if an exit is observed.
    ///
    /// A child being alive immediately after spawning is not enough: poll
    /// throughout the observation period to allow a death signal to arrive.
    async fn keeps_running(child: &mut Child, wait: Duration) -> bool {
        let deadline = Instant::now() + wait;
        while Instant::now() < deadline {
            if child.try_wait().expect("poll the child").is_some() {
                return false;
            }
            // Yield between polls so the test's runtime can keep running.
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        true
    }

    /// Kills the test child and waits at most [`EXIT_WAIT`] to reap it.
    /// Sending the kill alone would not collect the child's exit status or
    /// remove its remaining OS process record.
    async fn kill(mut child: Child) {
        child.start_kill().expect("kill the child");
        tokio::time::timeout(EXIT_WAIT, child.wait())
            .await
            .expect("the child exited")
            .expect("reap the child");
    }

    /// Whether the test child's `/proc/<pid>` entry has disappeared.
    /// An exited but unreaped child still has an entry, so checking this
    /// distinguishes completed cleanup from merely stopping the process.
    fn is_gone(pid: u32) -> bool {
        fs::metadata(format!("/proc/{pid}")).is_err()
    }

    /// Builds a `true` command whose child writes its PID to a pipe before
    /// exec. Returns the command and the pipe's read end for the test.
    ///
    /// The no-I/O test makes tokio panic after starting the child but before
    /// returning a `Child` handle. This PID report lets the test reap that
    /// exact child directly, without relying on tokio or scanning processes.
    fn true_reporting_pid() -> (Command, File) {
        // Both ends close at exec, so the executed program does not retain
        // these descriptors. The PID report happens before that point.
        let mut fds = [0; 2];
        // SAFETY: `fds` is a writable array of two `c_int`s, as `pipe2`
        // requires.
        let rc = unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_CLOEXEC) };
        assert_eq!(rc, 0, "pipe2: {}", io::Error::last_os_error());

        // SAFETY: `pipe2` succeeded, so both descriptors are open and owned by
        // nothing else.
        let (read, write) = unsafe { (OwnedFd::from_raw_fd(fds[0]), OwnedFd::from_raw_fd(fds[1])) };
        let mut command = Command::new("true");
        // SAFETY: the closure runs in the forked child before exec, where only
        // async-signal-safe calls are allowed. `getpid` and `write` are, and
        // `last_os_error` only reads `errno`. The write end is open in the
        // child because `O_CLOEXEC` closes it only at exec, and the closure
        // keeps the `OwnedFd` alive in this process until the command drops.
        unsafe {
            command.pre_exec(move || {
                let pid = libc::getpid().to_ne_bytes();
                if libc::write(write.as_raw_fd(), pid.as_ptr().cast(), pid.len())
                    != pid.len() as isize
                {
                    return Err(io::Error::last_os_error());
                }
                Ok(())
            });
        }
        (command, File::from(read))
    }

    /// Reaps the unregistered test child `pid` within [`EXIT_WAIT`].
    ///
    /// There is no tokio `Child` handle for this process, so the test must
    /// collect its exit directly. `WNOHANG` lets us poll with a deadline
    /// rather than block indefinitely inside `waitpid`.
    fn reap(pid: libc::pid_t) {
        let deadline = Instant::now() + EXIT_WAIT;
        loop {
            // SAFETY: `waitpid` takes a PID, a status pointer that may be
            // null, and flags. `pid` is this process's child, unregistered with
            // tokio, so nothing else reaps it.
            match unsafe { libc::waitpid(pid, ptr::null_mut(), libc::WNOHANG) } {
                0 => {
                    // The child has not exited yet. Retry only while the
                    // cleanup deadline allows it.
                    assert!(Instant::now() < deadline, "child {pid} was never reaped");
                    thread::sleep(Duration::from_millis(10));
                }
                reaped if reaped == pid => return,
                _ => panic!("waitpid({pid}): {}", io::Error::last_os_error()),
            }
        }
    }

    // CPS-1: the requesting thread can exit without killing the child.
    #[tokio::test]
    async fn child_outlives_the_thread_that_requested_it() {
        // The temporary thread calls `spawn_child`, so the child-spawner
        // thread actually forks the child. The temporary thread is joined
        // before this call returns.
        let mut child = spawn_from_short_lived_thread(sleep_with_death_signal("60"), spawn_child);

        // The requesting thread has exited, but the forking thread is still
        // alive, so the child must keep running.
        assert!(
            keeps_running(&mut child, SURVIVAL_WAIT).await,
            "the child died with the thread that requested it"
        );

        // Kill and reap the test child.
        kill(child).await;
    }

    // CPS-1: comparison case for the preceding test. Without `spawn_child`,
    // the child must die when the temporary thread that forked it exits.
    #[tokio::test]
    async fn child_forked_by_an_exiting_thread_is_killed() {
        // Spawn directly on the temporary OS thread. The helper joins that
        // thread before returning, so we know it has exited.
        let mut child =
            spawn_from_short_lived_thread(sleep_with_death_signal("60"), |mut command| {
                command.spawn()
            });

        // The test passes when the parent-death signal kills the child.
        // Bound the wait so a missing signal fails rather than hangs.
        let status = tokio::time::timeout(EXIT_WAIT, child.wait())
            .await
            .expect("the death signal killed the child")
            .expect("reap the child");
        assert_eq!(status.signal(), Some(libc::SIGKILL));
    }

    // CPS-1: a panicking spawn must not kill earlier children or prevent
    // later requests from being completed.
    #[tokio::test]
    async fn panicking_request_leaves_the_spawner_running() {
        // If the shared spawning thread exits, this child's parent-death
        // signal will kill it.
        let mut earlier = spawn_child(sleep_with_death_signal("60")).expect("spawn the child");

        // Force a panic on the spawning thread: tokio starts `true`, but
        // cannot register it with a runtime whose I/O driver is disabled.
        // Build and drop that runtime on a plain OS thread, because tokio
        // does not allow a runtime to be dropped inside an async task.
        let (result, mut reported_pid) = thread::spawn(|| {
            let without_io = tokio::runtime::Builder::new_current_thread()
                .build()
                .expect("build a runtime without I/O");
            let _runtime = without_io.enter();
            let (command, reported_pid) = true_reporting_pid();
            (spawn_child(command), reported_pid)
        })
        .join()
        .expect("the requesting thread panicked");

        // The child started, but failed registration left us without a Child
        // handle. Read the PID it sent through the pipe and reap it directly.
        let mut pid = [0; size_of::<libc::pid_t>()];
        reported_pid
            .read_exact(&mut pid)
            .expect("the unregistered child reported its PID");
        reap(libc::pid_t::from_ne_bytes(pid));

        // The spawning panic must have been caught and returned as an error.
        let error = result.expect_err("spawning into a runtime without I/O fails");
        assert!(
            error.to_string().contains("panicked"),
            "unexpected error: {error}"
        );

        // The earlier child must survive: the panic must not have ended
        // the shared thread that forked it.
        assert!(
            keeps_running(&mut earlier, SURVIVAL_WAIT).await,
            "an earlier child died after a request panicked"
        );

        // The spawning thread must also remain able to complete new requests.
        let later = spawn_child(Command::new("true")).expect("spawn after the panic");
        let status = tokio::time::timeout(EXIT_WAIT, later.wait_with_output())
            .await
            .expect("the later child exited")
            .expect("reap the later child")
            .status;
        assert!(status.success());

        // Kill and reap the long-running test child.
        kill(earlier).await;
    }

    /// Captured by a command's callback to make command destruction panic
    /// after spawning succeeds. Its panic carries [`PayloadPanicsOnDrop`]
    /// rather than a string, so disposing of that payload is tested too.
    struct PanicOnDrop;

    impl Drop for PanicOnDrop {
        fn drop(&mut self) {
            panic_any(PayloadPanicsOnDrop);
        }
    }

    /// The first panic's payload. Dropping it after catching that panic
    /// causes another panic, so the spawner must forget it instead.
    struct PayloadPanicsOnDrop;

    impl Drop for PayloadPanicsOnDrop {
        fn drop(&mut self) {
            panic!("the panic payload panicked on drop");
        }
    }

    // CPS-1, CPS-4: command destruction can panic after spawning succeeds.
    // Catching that panic must preserve the child handoff and keep the
    // spawning thread alive, even when the panic payload's drop panics too.
    #[tokio::test]
    async fn command_destructor_panic_leaves_the_spawner_running() {
        // This child's death signal detects whether command cleanup ends
        // the shared thread that forked it.
        let mut earlier = spawn_child(sleep_with_death_signal("60")).expect("spawn the child");

        // The callback itself succeeds. Its captured value panics only
        // when the parent drops the command after the spawn attempt.
        let mut command = Command::new("true");
        let capture = PanicOnDrop;
        // SAFETY: the closure runs in the forked child before exec and makes
        // no calls at all; it only holds `capture`, which is dropped in this
        // process when the command is, never in the child.
        unsafe {
            command.pre_exec(move || {
                let _ = &capture;
                Ok(())
            });
        }

        // A successful spawn must still return its child despite the
        // command-drop panic. `true` should then exit successfully.
        let child = spawn_child(command).expect("the child is handed over");
        let status = tokio::time::timeout(EXIT_WAIT, child.wait_with_output())
            .await
            .expect("the child exited")
            .expect("reap the child")
            .status;
        assert!(status.success());

        // Earlier children must not be killed by the cleanup panic.
        assert!(
            keeps_running(&mut earlier, SURVIVAL_WAIT).await,
            "an earlier child died after a command's destructor panicked"
        );

        // The shared thread must still receive and complete new requests.
        let later = spawn_child(Command::new("true")).expect("spawn after the panic");
        let status = tokio::time::timeout(EXIT_WAIT, later.wait_with_output())
            .await
            .expect("the later child exited")
            .expect("reap the later child")
            .status;
        assert!(status.success());

        // Kill and reap the long-running test child.
        kill(earlier).await;
    }

    // Check the test fixture itself: failure while owning a child must
    // leave that child killed and reaped, not running or left as a zombie.
    #[tokio::test]
    async fn failing_test_leaves_no_child_behind() {
        let child = spawn_child(sleep_with_death_signal("60")).expect("spawn the child");
        let pid = child.id().expect("a running child has a PID");

        // Move the handle into a task and force it to panic. Unwinding
        // drops the handle, invoking the fixture's kill-on-drop behavior.
        let failed = tokio::spawn(async move {
            let _child = child;
            panic!("forced test failure");
        })
        .await;
        assert!(failed.is_err(), "the forced failure did not fail");

        // Killing is not enough: an unreaped child still has a /proc entry.
        // Tokio reaps dropped children when its runtime next parks. Yield
        // while checking for removal, with a deadline on the cleanup wait.
        let deadline = Instant::now() + EXIT_WAIT;
        while !is_gone(pid) {
            assert!(
                Instant::now() < deadline,
                "child {pid} outlived the failed test or was never reaped"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    // CPS-2: spawning on another OS thread must still leave the caller's
    // runtime able to read the child's pipes and await its exit.
    #[tokio::test]
    async fn child_is_driven_by_the_callers_runtime() {
        // Capture stdout so the test exercises asynchronous pipe reading,
        // not just the process's exit status.
        let mut command = Command::new("echo");
        command.arg("hello").stdout(Stdio::piped());
        let child = spawn_child(command).expect("spawn the child");

        // Await output and exit on the test's runtime, not on the spawning
        // thread. A broken registration must fail rather than wait forever.
        let output = tokio::time::timeout(EXIT_WAIT, child.wait_with_output())
            .await
            .expect("the child exited")
            .expect("read the child's output");
        assert!(output.status.success());
        assert_eq!(output.stdout, b"hello\n");
    }

    // An ordinary spawn failure must reach the caller with its original
    // I/O error kind, rather than being mistaken for a panic.
    #[tokio::test]
    async fn spawn_errors_propagate() {
        let error = spawn_child(Command::new("/nonexistent/hyperactor-child-process"))
            .expect_err("spawning a nonexistent program fails");
        assert_eq!(error.kind(), io::ErrorKind::NotFound);
    }

    // CPS-2: a caller without an entered tokio runtime gets an error.
    // This plain test runs without the runtime provided by `tokio::test`.
    #[test]
    fn spawning_outside_a_runtime_fails() {
        spawn_child(Command::new("true")).expect_err("spawning outside a runtime fails");
    }

    /// Sets up the worker-retirement scenario from meta-pytorch/monarch#5248.
    ///
    /// A one-worker runtime requests a death-signal child using `spawn`.
    /// `block_in_place` then moves the worker role to another OS thread,
    /// letting the original thread retire from the blocking pool. The test
    /// must observe that original thread's exit, not merely the role moving.
    ///
    /// Returns both the child and the still-live runtime. The calling test
    /// can then check the child while ruling out runtime shutdown as the
    /// cause of its death. `spawn` selects our helper or ordinary spawning.
    fn spawn_then_retire_worker(spawn: fn(Command) -> io::Result<Child>) -> (Child, Runtime) {
        // Keep the blocking pool's idle-thread lifetime short so the old
        // worker can retire during the test. The runtime itself stays alive.
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .thread_keep_alive(Duration::from_millis(100))
            .enable_all()
            .build()
            .expect("build a one-worker runtime");

        // `block_on` alone runs its main future on the calling thread.
        // Use `tokio::spawn` to make the request from a runtime worker,
        // and record that worker's OS thread ID before the request.
        let (child, worker) = runtime.block_on(async {
            tokio::spawn(async move {
                let worker = gettid();
                (
                    spawn(sleep_with_death_signal("60")).expect("spawn the child"),
                    worker,
                )
            })
            .await
            .expect("the spawning task panicked")
        });

        runtime.block_on(async {
            // `block_in_place` lets another thread take over the worker's
            // async work. A short call can reclaim that work before the
            // replacement starts, so retry until a probe sees a new thread.
            for _ in 0..100 {
                tokio::spawn(async {
                    tokio::task::block_in_place(|| thread::sleep(Duration::from_millis(10)));
                })
                .await
                .expect("the blocking task panicked");

                // Run the probe as a task too, so its ID belongs to the
                // current worker rather than the thread driving `block_on`.
                let current = tokio::spawn(async { gettid() })
                    .await
                    .expect("the probe task panicked");
                if current != worker {
                    return;
                }
            }
            panic!("the worker role never left thread {worker}");
        });

        // The role has moved, but the old OS thread may still be alive in
        // the blocking pool. Wait for its /proc entry to disappear before
        // checking the child; fail if the thread never actually exits.
        let deadline = Instant::now() + EXIT_WAIT;
        while fs::metadata(format!("/proc/self/task/{worker}")).is_ok() {
            assert!(Instant::now() < deadline, "thread {worker} never retired");
            thread::sleep(Duration::from_millis(50));
        }
        (child, runtime)
    }

    // CPS-1: retiring the requesting tokio worker must not kill a child
    // forked by the process-lifetime spawning thread.
    #[test]
    fn child_survives_its_worker_retiring_after_block_in_place() {
        // Return only once the worker that requested the spawn has exited.
        // `spawn_child` forks on the shared OS thread instead of that worker.
        let (mut child, runtime) = spawn_then_retire_worker(spawn_child);
        runtime.block_on(async {
            // The runtime remains alive, but the original worker is gone.
            // Its retirement must not trigger this child's death signal.
            assert!(
                keeps_running(&mut child, SURVIVAL_WAIT).await,
                "the child died when its spawning worker retired"
            );

            // Kill and reap the test child while its runtime is still alive.
            kill(child).await;
        });
    }

    // CPS-1: comparison case. Direct spawning on a tokio worker must
    // reproduce the bug: worker retirement kills the death-signal child.
    #[test]
    fn child_forked_on_a_worker_dies_when_it_retires() {
        // Use ordinary `spawn`, making that worker the child's forking
        // thread. The helper waits for the worker's OS thread to exit.
        let (mut child, runtime) = spawn_then_retire_worker(|mut command| command.spawn());
        runtime.block_on(async {
            // The parent process and runtime are alive. The test passes
            // only if the worker's exit nevertheless kills the child.
            let status = tokio::time::timeout(EXIT_WAIT, child.wait())
                .await
                .expect("the death signal killed the child")
                .expect("reap the child");
            assert_eq!(status.signal(), Some(libc::SIGKILL));
        });
    }

    // ── Child-spawner invariant coverage ──
    // CPS-1: child_outlives_the_thread_that_requested_it (witnessed by
    //        child_forked_by_an_exiting_thread_is_killed),
    //        child_survives_its_worker_retiring_after_block_in_place (witnessed
    //        by child_forked_on_a_worker_dies_when_it_retires),
    //        panicking_request_leaves_the_spawner_running,
    //        command_destructor_panic_leaves_the_spawner_running
    // CPS-2: child_is_driven_by_the_callers_runtime, spawning_outside_a_runtime_fails
    // CPS-3: by inspection: `spawn_child` has no path that calls
    //        `Command::spawn` on the calling thread
    // CPS-4: by inspection: `spawn_child` is synchronous;
    //        command_destructor_panic_leaves_the_spawner_running checks that a
    //        spawned child is handed over even when the command's drop panics
    // Fixture cleanup: failing_test_leaves_no_child_behind
}
