# Appendix: pytokio (Python/Rust async bridge)

# pytokio

Monarch needs a way for Python code to kick off Rust futures and later await the result. `pytokio.rs` provides that.

It does three things:

1. lets Rust bundle up a future as a Python object (`PythonTask`);
2. runs the future on a Tokio runtime;
3. delivers the result back to Python in an `await`-friendly way.

## 1. Core types

There are three main types in `pytokio.rs`:

1. `PythonTask` (Rust struct, not exposed directly to Python)
2. `PyPythonTask` (`#[pyclass(name = "PythonTask")]` — the Python-visible wrapper)
3. `PyShared` (the handle you actually await on the Python side)

## 2. `PythonTask`: "I have a Rust future"

At the bottom we have:

```rust
pub(crate) struct PythonTask {
    future: Mutex<Pin<Box<dyn Future<Output = PyResult<PyObject>> + Send + 'static>>>,
    traceback: Option<PyObject>,
}
```

This is just:

- a boxed Rust future that produces a `PyResult<PyObject>`, and
- an optional captured Python traceback.

Bindings create one of these by calling `PyPythonTask::new(async move { ... })`, which in turn makes a `PythonTask`.

## 3. `PyPythonTask`: the Python-visible task

```rust
#[pyclass(
    name = "PythonTask",
    module = "monarch._rust_bindings.monarch_hyperactor.pytokio"
)]
pub struct PyPythonTask {
    inner: Option<PythonTask>,
}
```

Important points:

- it owns exactly one `PythonTask`;
- that task is **consumed** when you run it (via `spawn`, `block_on`, etc.);
- it offers a few driving methods: `spawn()`, `spawn_abortable()`, `block_on()`, `__await__`, `with_timeout(...)`.

This is what the higher-level bindings return.

## 4. How the future actually runs (`spawn` path)

```rust
pub(crate) fn spawn(&mut self) -> PyResult<PyShared> {
    let (tx, rx) = watch::channel(None);
    let traceback = self.traceback()?;
    let traceback1 = self.traceback()?;
    let task = self.take_task()?;
    let handle = get_tokio_runtime().spawn(async move {
        send_result(tx, task.await, traceback1);
    });
    Ok(PyShared {
        rx,
        handle,
        abort: false,
        traceback,
    })
}
```

What that does:

1. open a Tokio watch channel `(tx, rx)`;
2. consume the underlying Rust future (`take_task()`);
3. run that future on the shared Tokio runtime (`get_tokio_runtime().spawn(...)`);
4. when it completes, push the result into the watch channel (`send_result(...)`);
5. return a `PyShared` that holds the receiver and the join handle.

So the Rust future is running in the background, and Python will wait on the receiver.

## 5. `PyShared`: what Python actually awaits

```rust
#[pyclass(
    name = "Shared",
    module = "monarch._rust_bindings.monarch_hyperactor.pytokio"
)]
pub struct PyShared {
    rx: watch::Receiver<Option<PyResult<PyObject>>>,
    handle: JoinHandle<()>,
    abort: bool,
    traceback: Option<PyObject>,
}
```

Its `task()` / `__await__` path just waits for `rx.changed()` — i.e. waits until the Rust side has written the result — and then hands that result back to Python. If `abort` is `true`, dropping it will cancel the Tokio task.

If a spawned task errors and nobody awaits it, pytokio logs the error instead of silently dropping it. If you set `ENABLE_UNAWAITED_PYTHON_TASK_TRACEBACK=1`, it will also log where the task was created, which makes "why did this background thing fail?" a lot easier to answer.

## 6. `block_on(...)` for sync Python

```rust
fn block_on(mut slf: PyRefMut<PyPythonTask>, py: Python<'_>) -> PyResult<PyObject> {
    let task = slf.take_task()?;
    drop(slf);
    signal_safe_block_on(py, task)?
}
```

This is the same idea, but instead of returning a `PyShared`, it blocks the current thread and runs the future to completion, using `signal_safe_block_on(...)` so Ctrl‑C still works.

## 7. `from_coroutine(...)`: wrap a Python coroutine in the same machinery

There is also:

```rust
#[staticmethod]
fn from_coroutine(py: Python<'_>, coro: PyObject) -> PyResult<PyPythonTask> { ... }
```

This is the "other direction": you already have a **Python** coroutine, but you want to run it under the same task runner so it can await these Rust-backed tasks and keep the Monarch context vars. The implementation:

- calls the coroutine's `__await__`,
- drives it in a loop,
- when the coroutine yields another `PythonTask`, it awaits that on Tokio,
- and it keeps restoring the Monarch `context()` inside the loop.

The `context()` preservation is critical for the actor model: when you call `context()` inside a `PythonTask`, you get the actor instance that spawned the task, even though you're running on an arbitrary Tokio thread.

So both "Rust future exposed to Python" and "Python coroutine driven by the same runner" go through the same `PyPythonTask` shape.

## 8. What it's for

Every v1 binding that needs to do async Rust work does the same thing:

1. build an async Rust block that calls the real API;
2. wrap it in `PyPythonTask::new(...)`;
3. return that to Python.

Then user code can decide to `await`, `.spawn()`, or `.block_on()`.
