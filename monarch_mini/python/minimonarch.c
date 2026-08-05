/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//
// minimonarch.c — a small CPython extension that binds the minimonarch C API
// (minimonarch.h) into asyncio-friendly Python objects, to make writing tests
// against minimonarch easier.
//
// Surface. Single small values (idents, reasons) are bytes and copied;
// multipart message bodies are lists of minimonarch.bytearray and moved.
//
//     ba = minimonarch.bytearray
//     a = Actor(b'ident@root')            # creates (or reuses) a context
//     a.send(b'ident@root', [ba(b'a'), ba(b'b')])
//     parts = await a.next()              # -> list[minimonarch.bytearray]
//     a.join('inproc://a', 'child')
//     b = Actor(b'other@root')
//     b.serve('inproc://b', 'parent')
//     h = a.monitor(b'ident@root')        # -> MonitorHandle (unimplemented)
//     h.cancel()
//     a.die(b'reason')
//
// Design notes:
//
//   * Contexts live in a contextvars.ContextVar. The first Actor created in a
//     given context lazily creates an mm_ctx_t + a single mm_poller_t and
//     installs that poller's wakeup fd on the running asyncio loop. Every actor
//     in the context subscribes to that one poller.
//
//   * The poller's fd reader drains mm_poller_next() and routes each delivered
//     message onto the owning Actor's asyncio.Queue. next() is simply that
//     queue's get(): it returns a buffered message immediately or suspends
//     until one arrives. A message is a list of minimonarch.bytearray, each
//     adopting a received buffer zero-copy; it can be moved straight back into
//     another send(), so a buffer can ping-pong without ever being copied.
//
//   * No message part ever holds a Python reference, so no deleter ever needs
//     the GIL — on any thread. Single-value args (a bytes ident/reason) are
//     copied into a C buffer; multipart bodies are lists of bytearray whose
//     storage is moved into the parts. Either way the part owns C memory and
//     frees it with a plain free().

#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <stddef.h>
#include <time.h>

#include "minimonarch.h"

// ---------------------------------------------------------------------------
// Module-level cached objects
// ---------------------------------------------------------------------------

static PyObject* g_ctx_var; // ContextVar holding the current Context
static PyObject* g_get_running_loop; // asyncio.get_running_loop
static PyObject* g_queue_type; // asyncio.Queue
static PyObject* g_pump_func; // C reader callback registered with add_reader

// Actor.next() busy-pumps the poller for this long before falling back to the
// fd/queue wait. This catches an imminent message directly off the poller and
// skips the eventfd/epoll wakeup — and, by staying busy across the routing
// thread's own park-wakeup, absorbs that latency too, roughly halving
// round-trip time (~18 us -> ~8 us locally). 10 us is about the routing
// thread's wakeup, which is the floor the message waits on; spinning longer
// buys nothing, and it only spins while a message is pending (then sleeps on
// the fd), so an idle consumer doesn't burn a core.
//
// This is safe only because the routing thread never blocks on the GIL: every
// message part is a moved minimonarch.bytearray whose deleter is a plain free()
// (no Python refcounting), so the runtime never needs the GIL the spinning
// consumer holds. Otherwise the spin would starve the routing thread.
#define NEXT_SPIN_NS 10000L

static PyTypeObject ContextType;
static PyTypeObject ActorType;
static PyTypeObject MonitorHandleType;

// ---------------------------------------------------------------------------
// Object layouts
// ---------------------------------------------------------------------------

typedef struct {
  PyObject_HEAD mm_ctx_t ctx;
  mm_poller_t poller;
  int fd;
  PyObject* loop; // strong ref to the asyncio loop
  PyObject* queues; // dict: index(int) -> asyncio.Queue
  PyObject* token; // ContextVar token from the Set that installed us
  size_t next_index;
  int reader_installed;
  int closed; // resources released; shutdown is idempotent
  PyObject* weakreflist; // so the loop's reader can hold us weakly
} Context;

typedef struct {
  PyObject_HEAD mm_actor_t actor;
  Context* ctx; // strong ref
  size_t index; // poller subscription index
  PyObject* queue; // this actor's asyncio.Queue (also held in ctx->queues)
} Actor;

typedef struct {
  PyObject_HEAD mm_monitor_handle_t handle;
} MonitorHandle;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

static int set_mm_error(void) {
  PyErr_SetString(PyExc_RuntimeError, mm_last_error());
  return -1;
}

// ---------------------------------------------------------------------------
// bytearray — a writable, growable byte buffer (like Python's bytearray) whose
// C-owned storage can be *moved* into a message part. After a move the part
// owns the allocation and frees it with a plain free() — no GIL on the runtime
// thread — and the bytearray is left empty.
//
// The C type is named ByteArray; it is exposed to Python as
// minimonarch.bytearray.
// ---------------------------------------------------------------------------

typedef struct {
  PyObject_HEAD char* data; // malloc'd; NULL when empty/moved
  Py_ssize_t len;
  Py_ssize_t cap;
  Py_ssize_t exports; // outstanding buffer-protocol views; block resize/move
} ByteArray;

static PyTypeObject ByteArrayType;

static void free_deleter(void* ctx) {
  free(ctx);
}

static int bytearray_reserve(ByteArray* b, Py_ssize_t need) {
  if (need <= b->cap) {
    return 0;
  }
  Py_ssize_t cap = b->cap ? b->cap : 8;
  while (cap < need) {
    cap *= 2;
  }
  char* grown = realloc(b->data, (size_t)cap);
  if (!grown) {
    PyErr_NoMemory();
    return -1;
  }
  b->data = grown;
  b->cap = cap;
  return 0;
}

// Resizing or moving while a memoryview is exported would dangle that view.
static int bytearray_check_mutable(ByteArray* b) {
  if (b->exports > 0) {
    PyErr_SetString(
        PyExc_BufferError,
        "bytearray has exported views; cannot resize or move");
    return -1;
  }
  return 0;
}

// Move the storage into `out` (free deleter) and reset the ByteArray to empty.
// Returns -1 (exception set) if views are outstanding.
static int bytearray_move_to_part(ByteArray* b, mm_msg_part_t* out) {
  if (bytearray_check_mutable(b) < 0) {
    return -1;
  }
  out->data = b->data;
  out->len = (size_t)b->len;
  out->deleter = b->data ? free_deleter : NULL;
  out->deleter_ctx = b->data;
  b->data = NULL;
  b->len = 0;
  b->cap = 0;
  return 0;
}

static int ByteArray_init(ByteArray* self, PyObject* args, PyObject* kwds) {
  static char* kw[] = {"source", NULL};
  PyObject* source = NULL;
  if (!PyArg_ParseTupleAndKeywords(args, kwds, "|O", kw, &source)) {
    return -1;
  }
  if (source == NULL || source == Py_None) {
    return 0; // empty
  }
  if (PyLong_Check(source)) {
    Py_ssize_t count = PyLong_AsSsize_t(source);
    if (count < 0) {
      if (!PyErr_Occurred()) {
        PyErr_SetString(
            PyExc_ValueError, "bytearray count must be non-negative");
      }
      return -1;
    }
    if (bytearray_reserve(self, count) < 0) {
      return -1;
    }
    memset(self->data, 0, (size_t)count);
    self->len = count;
    return 0;
  }
  // Any bytes-like: copy its contents in.
  Py_buffer view;
  if (PyObject_GetBuffer(source, &view, PyBUF_SIMPLE) < 0) {
    return -1;
  }
  if (bytearray_reserve(self, view.len) < 0) {
    PyBuffer_Release(&view);
    return -1;
  }
  memcpy(self->data, view.buf, (size_t)view.len);
  self->len = view.len;
  PyBuffer_Release(&view);
  return 0;
}

static void ByteArray_dealloc(ByteArray* self) {
  free(self->data);
  Py_TYPE(self)->tp_free((PyObject*)self);
}

static Py_ssize_t ByteArray_length(ByteArray* self) {
  return self->len;
}

static PyObject* ByteArray_item(ByteArray* self, Py_ssize_t i) {
  if (i < 0) {
    i += self->len;
  }
  if (i < 0 || i >= self->len) {
    PyErr_SetString(PyExc_IndexError, "bytearray index out of range");
    return NULL;
  }
  return PyLong_FromLong((unsigned char)self->data[i]);
}

static int ByteArray_ass_item(ByteArray* self, Py_ssize_t i, PyObject* value) {
  if (i < 0) {
    i += self->len;
  }
  if (i < 0 || i >= self->len) {
    PyErr_SetString(PyExc_IndexError, "bytearray index out of range");
    return -1;
  }
  if (!value) {
    PyErr_SetString(PyExc_TypeError, "cannot delete bytearray items");
    return -1;
  }
  long byte = PyLong_AsLong(value);
  if (byte == -1 && PyErr_Occurred()) {
    return -1;
  }
  if (byte < 0 || byte > 255) {
    PyErr_SetString(PyExc_ValueError, "byte must be in range(0, 256)");
    return -1;
  }
  self->data[i] = (char)byte;
  return 0;
}

static PyObject* ByteArray_append(ByteArray* self, PyObject* arg) {
  if (bytearray_check_mutable(self) < 0) {
    return NULL;
  }
  long byte = PyLong_AsLong(arg);
  if (byte == -1 && PyErr_Occurred()) {
    return NULL;
  }
  if (byte < 0 || byte > 255) {
    PyErr_SetString(PyExc_ValueError, "byte must be in range(0, 256)");
    return NULL;
  }
  if (bytearray_reserve(self, self->len + 1) < 0) {
    return NULL;
  }
  self->data[self->len++] = (char)byte;
  Py_RETURN_NONE;
}

static PyObject* ByteArray_extend(ByteArray* self, PyObject* arg) {
  if (bytearray_check_mutable(self) < 0) {
    return NULL;
  }
  Py_buffer view;
  if (PyObject_GetBuffer(arg, &view, PyBUF_SIMPLE) < 0) {
    return NULL;
  }
  if (bytearray_reserve(self, self->len + view.len) < 0) {
    PyBuffer_Release(&view);
    return NULL;
  }
  memcpy(self->data + self->len, view.buf, (size_t)view.len);
  self->len += view.len;
  PyBuffer_Release(&view);
  Py_RETURN_NONE;
}

static PyObject* ByteArray_tobytes(
    ByteArray* self,
    PyObject* Py_UNUSED(ignored)) {
  return PyBytes_FromStringAndSize(self->data, self->len);
}

static int ByteArray_getbuffer(PyObject* self, Py_buffer* view, int flags) {
  ByteArray* b = (ByteArray*)self;
  if (PyBuffer_FillInfo(view, self, b->data, b->len, /*readonly=*/0, flags) <
      0) {
    return -1;
  }
  b->exports++;
  return 0;
}

static void ByteArray_releasebuffer(PyObject* self, Py_buffer* view) {
  (void)view;
  ((ByteArray*)self)->exports--;
}

// Content equality against any bytes-like (so a received bytearray compares
// equal to the bytes that were sent). Only == / != are defined.
static PyObject*
ByteArray_richcompare(PyObject* self, PyObject* other, int op) {
  if (op != Py_EQ && op != Py_NE) {
    Py_RETURN_NOTIMPLEMENTED;
  }
  Py_buffer a;
  Py_buffer b;
  if (PyObject_GetBuffer(self, &a, PyBUF_SIMPLE) < 0) {
    PyErr_Clear();
    Py_RETURN_NOTIMPLEMENTED;
  }
  if (PyObject_GetBuffer(other, &b, PyBUF_SIMPLE) < 0) {
    PyErr_Clear();
    PyBuffer_Release(&a);
    Py_RETURN_NOTIMPLEMENTED;
  }
  int equal = a.len == b.len &&
      (a.len == 0 || memcmp(a.buf, b.buf, (size_t)a.len) == 0);
  PyBuffer_Release(&a);
  PyBuffer_Release(&b);
  if ((op == Py_EQ) == (equal != 0)) {
    Py_RETURN_TRUE;
  }
  Py_RETURN_FALSE;
}

static PySequenceMethods ByteArray_as_sequence = {
    .sq_length = (lenfunc)ByteArray_length,
    .sq_item = (ssizeargfunc)ByteArray_item,
    .sq_ass_item = (ssizeobjargproc)ByteArray_ass_item,
};

static PyBufferProcs ByteArray_as_buffer = {
    .bf_getbuffer = ByteArray_getbuffer,
    .bf_releasebuffer = ByteArray_releasebuffer,
};

static PyMethodDef ByteArray_methods[] = {
    {"append", (PyCFunction)ByteArray_append, METH_O, "append(byte) -> None"},
    {"extend",
     (PyCFunction)ByteArray_extend,
     METH_O,
     "extend(bytes-like) -> None"},
    {"tobytes",
     (PyCFunction)ByteArray_tobytes,
     METH_NOARGS,
     "tobytes() -> bytes"},
    {NULL, NULL, 0, NULL},
};

static PyTypeObject ByteArrayType = {
    PyVarObject_HEAD_INIT(NULL, 0).tp_name = "minimonarch.bytearray",
    .tp_basicsize = sizeof(ByteArray),
    .tp_dealloc = (destructor)ByteArray_dealloc,
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_doc =
        "A writable, growable byte buffer; send() moves it into the message.",
    .tp_richcompare = ByteArray_richcompare,
    .tp_as_sequence = &ByteArray_as_sequence,
    .tp_as_buffer = &ByteArray_as_buffer,
    .tp_methods = ByteArray_methods,
    .tp_init = (initproc)ByteArray_init,
    .tp_new = PyType_GenericNew,
};

// Move a single minimonarch.bytearray argument into `out`. Type-checks and, on
// success, leaves the bytearray empty. Returns -1 (exception set) otherwise.
static int arg_move(PyObject* obj, mm_msg_part_t* out) {
  if (!PyObject_TypeCheck(obj, &ByteArrayType)) {
    PyErr_SetString(PyExc_TypeError, "expected a minimonarch.bytearray");
    return -1;
  }
  return bytearray_move_to_part((ByteArray*)obj, out);
}

// Copy a single small bytes value (an ident, a reason) into a C-owned part with
// a plain free() deleter. Single-value arguments are bytes (and copied) rather
// than bytearray; only multipart message bodies are moved bytearrays. Returns
// -1 (exception set) on a non-bytes value or allocation failure.
static int bytes_to_part(PyObject* obj, mm_msg_part_t* out) {
  if (!PyBytes_Check(obj)) {
    PyErr_SetString(PyExc_TypeError, "expected bytes");
    return -1;
  }
  Py_ssize_t len = PyBytes_GET_SIZE(obj);
  void* buf = malloc((size_t)(len ? len : 1));
  if (!buf) {
    PyErr_NoMemory();
    return -1;
  }
  if (len) {
    memcpy(buf, PyBytes_AS_STRING(obj), (size_t)len);
  }
  out->data = buf;
  out->len = (size_t)len;
  out->deleter = free_deleter;
  out->deleter_ctx = buf;
  return 0;
}

// Build a message-part array by moving each minimonarch.bytearray in `seq`
// (storage taken over, each bytearray left empty). On error, releases parts
// already taken via their free() deleters and returns NULL.
static mm_msg_part_t* build_parts(PyObject* seq, size_t* out_n) {
  Py_ssize_t n = PySequence_Size(seq);
  if (n < 0) {
    return NULL;
  }
  mm_msg_part_t* arr = PyMem_Malloc((size_t)n * sizeof(mm_msg_part_t));
  if (!arr) {
    PyErr_NoMemory();
    return NULL;
  }
  for (Py_ssize_t i = 0; i < n; i++) {
    PyObject* item = PySequence_GetItem(seq, i);
    int ok = item && arg_move(item, &arr[i]) == 0;
    Py_XDECREF(item);
    if (!ok) {
      for (Py_ssize_t j = 0; j < i; j++) {
        if (arr[j].deleter) {
          arr[j].deleter(arr[j].deleter_ctx);
        }
      }
      PyMem_Free(arr);
      return NULL;
    }
  }
  *out_n = (size_t)n;
  return arr;
}

// Adopt a received part's buffer into a new minimonarch.bytearray, zero-copy:
// the bytearray takes over the malloc'd storage and will free()/realloc() it
// like any other bytearray. (Every part this binding produces is malloc-owned
// with a free() deleter, so adopting the pointer is equivalent to its deleter.)
// Returns a new reference, or NULL (with the buffer freed) on failure. Because
// the result is itself a bytearray, a received message can be moved straight
// back into another send() — the buffer is reused, never copied.
static PyObject* bytearray_adopt(mm_msg_part_t* part) {
  ByteArray* b = PyObject_New(ByteArray, &ByteArrayType);
  if (!b) {
    if (part->deleter) {
      part->deleter(part->deleter_ctx);
    }
    return NULL;
  }
  b->data = (char*)part->data;
  b->len = (Py_ssize_t)part->len;
  b->cap = (Py_ssize_t)part->len;
  b->exports = 0;
  return (PyObject*)b;
}

// ---------------------------------------------------------------------------
// Message delivery (runs on the asyncio loop thread, via the fd reader)
// ---------------------------------------------------------------------------

static void deliver_to_queue(Context* c, size_t index, PyObject* msg) {
  PyObject* key = PyLong_FromSize_t(index);
  if (!key) {
    PyErr_Clear();
    return;
  }
  PyObject* queue = PyDict_GetItemWithError(c->queues, key); // borrowed
  Py_DECREF(key);
  if (!queue) {
    return; // no actor subscribed at this index (already destroyed)
  }

  // The queue is unbounded, so put_nowait never blocks. A pending get() (i.e.
  // a coroutine awaiting next()) is woken; otherwise the message is buffered.
  PyObject* r = PyObject_CallMethod(queue, "put_nowait", "O", msg);
  Py_XDECREF(r);
  if (!r) {
    PyErr_Clear();
  }
}

// Build a list of minimonarch.bytearray from `parts` (each adopting its buffer
// zero-copy) and route it to `index`'s queue. Must hold the GIL.
static void
route_one(Context* c, size_t index, mm_msg_part_t* parts, size_t n) {
  PyObject* msg = PyList_New((Py_ssize_t)n);
  for (size_t i = 0; i < n; i++) {
    PyObject* part = bytearray_adopt(&parts[i]);
    if (msg && part) {
      PyList_SET_ITEM(msg, (Py_ssize_t)i, part); // steals part
    } else {
      Py_XDECREF(part);
    }
  }
  if (msg) {
    deliver_to_queue(c, index, msg);
    Py_DECREF(msg);
  } else {
    PyErr_Clear();
  }
}

// Drain every currently-available message from the poller, routing each to its
// queue. Returns 1 if a message destined for `want_index` was delivered, else
// 0. Pass (size_t)-1 for want_index to ignore the result. Must hold the GIL.
static int pump_drain(Context* c, size_t want_index) {
  mm_msg_part_t stack_parts[16];
  mm_msg_part_t* parts = stack_parts;
  size_t cap = 16;
  int got_want = 0;

  for (;;) {
    size_t index = 0;
    size_t n = 0;
    mm_err_t err = mm_poller_next(c->poller, &index, parts, cap, &n);
    if (err == MM_ENOMSG) {
      break;
    }
    if (err == MM_EBUFSZ) {
      if (parts != stack_parts) {
        PyMem_Free(parts);
      }
      parts = PyMem_Malloc(n * sizeof(mm_msg_part_t));
      if (!parts) {
        PyErr_NoMemory();
        PyErr_WriteUnraisable(NULL);
        return got_want;
      }
      cap = n;
      continue; // message not consumed; retry with a bigger buffer
    }
    if (err != MM_OK) {
      break;
    }

    route_one(c, index, parts, n);
    if (index == want_index) {
      got_want = 1;
    }
  }

  if (parts != stack_parts) {
    PyMem_Free(parts);
  }
  return got_want;
}

// asyncio fd reader: drains the poller and routes each message to the queue
// registered for its index. Receives the Context as a weakref so the loop
// never keeps the Context alive (which would form an uncollectable cycle).

static PyObject* mm_pump(PyObject* self, PyObject* wref) {
  (void)self;
  PyObject* obj = PyWeakref_GetObject(wref); // borrowed
  if (obj != Py_None) {
    pump_drain((Context*)obj, (size_t)-1);
  }
  Py_RETURN_NONE;
}

// ---------------------------------------------------------------------------
// Context
// ---------------------------------------------------------------------------

static Context* context_new(void) {
  PyObject* loop = PyObject_CallNoArgs(g_get_running_loop);
  if (!loop) {
    return NULL;
  }
  Context* c = PyObject_New(Context, &ContextType);
  if (!c) {
    Py_DECREF(loop);
    return NULL;
  }
  c->ctx = NULL;
  c->poller = NULL;
  c->fd = -1;
  c->loop = loop;
  c->queues = NULL;
  c->token = NULL;
  c->next_index = 0;
  c->reader_installed = 0;
  c->closed = 0;
  c->weakreflist = NULL;

  if (mm_ctx_create(&c->ctx) != MM_OK) {
    set_mm_error();
    Py_DECREF(c);
    return NULL;
  }
  if (mm_poller_create(c->ctx, &c->fd, &c->poller) != MM_OK) {
    set_mm_error();
    Py_DECREF(c);
    return NULL;
  }
  c->queues = PyDict_New();
  if (!c->queues) {
    Py_DECREF(c);
    return NULL;
  }

  // Register the reader with a *weak* reference to the Context. A strong ref
  // here would make the loop pin the Context, forming an uncollectable cycle
  // (loop -> reader -> ctx -> queues -> Queue -> loop). With a weakref the loop
  // never keeps the Context alive; the contextvar and live Actors do.
  PyObject* wref = PyWeakref_NewRef((PyObject*)c, NULL);
  if (!wref) {
    Py_DECREF(c);
    return NULL;
  }
  PyObject* res = PyObject_CallMethod(
      c->loop, "add_reader", "iOO", c->fd, g_pump_func, wref);
  Py_DECREF(wref);
  if (!res) {
    Py_DECREF(c);
    return NULL;
  }
  Py_DECREF(res);
  c->reader_installed = 1;
  return c;
}

// Return the Context for the current contextvars context, creating and
// installing one if absent. Returns a new reference.
static Context* context_current(void) {
  PyObject* cur = NULL;
  if (PyContextVar_Get(g_ctx_var, NULL, &cur) < 0) {
    return NULL;
  }
  if (cur != NULL) {
    return (Context*)cur; // new ref from PyContextVar_Get
  }
  Context* c = context_new();
  if (!c) {
    return NULL;
  }
  PyObject* token = PyContextVar_Set(g_ctx_var, (PyObject*)c);
  if (!token) {
    Py_DECREF(c);
    return NULL;
  }
  c->token = token; // kept so close() can reset the var in this context
  return c; // we still own the creation reference
}

// Tear down the minimonarch runtime for this context: detach the fd reader,
// destroy the poller, then the context itself (mm_ctx_destroy flushes pending
// messages and joins the runtime thread). Idempotent. Actor objects are NOT
// touched here — they survive as Python objects, and the `closed` flag makes
// their methods raise instead of dereferencing the destroyed runtime.
static void context_shutdown(Context* self) {
  if (self->closed) {
    return;
  }
  self->closed = 1;

  if (self->reader_installed && self->loop) {
    PyObject* r =
        PyObject_CallMethod(self->loop, "remove_reader", "i", self->fd);
    Py_XDECREF(r);
    if (!r) {
      PyErr_Clear();
    }
    self->reader_installed = 0;
  }

  if (self->poller) {
    mm_poller_destroy(self->poller);
    self->poller = NULL;
  }
  if (self->ctx) {
    // ctx_destroy flushes and joins the runtime thread; its part deleters are
    // plain free()s now, so no GIL coordination is needed. Release the GIL
    // anyway so we don't hold it across the join.
    mm_ctx_t ctx = self->ctx;
    self->ctx = NULL;
    Py_BEGIN_ALLOW_THREADS mm_ctx_destroy(ctx);
    Py_END_ALLOW_THREADS
  }
}

static void Context_dealloc(Context* self) {
  if (self->weakreflist) {
    PyObject_ClearWeakRefs((PyObject*)self);
  }
  context_shutdown(self);
  Py_CLEAR(self->loop);
  Py_CLEAR(self->queues);
  Py_CLEAR(self->token);
  Py_TYPE(self)->tp_free((PyObject*)self);
}

static PyTypeObject ContextType = {
    PyVarObject_HEAD_INIT(NULL, 0).tp_name = "minimonarch._Context",
    .tp_basicsize = sizeof(Context),
    .tp_dealloc = (destructor)Context_dealloc,
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_doc = "Internal minimonarch context (one per contextvars context).",
    .tp_weaklistoffset = offsetof(Context, weakreflist),
};

// ---------------------------------------------------------------------------
// Actor
// ---------------------------------------------------------------------------

static int Actor_init(Actor* self, PyObject* args, PyObject* kwds) {
  static char* kw[] = {"ident", "gateway", NULL};
  PyObject* ident = Py_None;
  int gateway = 0; // 'p' parses any truthy value into 0/1
  if (!PyArg_ParseTupleAndKeywords(args, kwds, "|Op", kw, &ident, &gateway)) {
    return -1;
  }

  Context* c = context_current(); // new ref
  if (!c) {
    return -1;
  }
  self->ctx = c; // steal the reference
  self->actor = NULL;
  self->index = 0;
  self->queue = NULL;

  mm_actor_t actor = NULL;
  mm_err_t err;
  if (ident == Py_None) {
    Py_BEGIN_ALLOW_THREADS err =
        mm_actor_create(c->ctx, NULL, (bool)gateway, &actor);
    Py_END_ALLOW_THREADS
  } else {
    mm_msg_part_t p;
    if (bytes_to_part(ident, &p) < 0) {
      return -1;
    }
    Py_BEGIN_ALLOW_THREADS err =
        mm_actor_create(c->ctx, &p, (bool)gateway, &actor);
    Py_END_ALLOW_THREADS
  }
  if (err != MM_OK) {
    set_mm_error();
    return -1;
  }
  self->actor = actor;
  self->index = c->next_index++;

  if (mm_poller_subscribe(c->poller, self->index, actor) != MM_OK) {
    return set_mm_error();
  }

  self->queue = PyObject_CallNoArgs(g_queue_type);
  if (!self->queue) {
    return -1;
  }

  // Register the queue under our index so the pump can route to it. The context
  // and the actor both hold a strong reference to the same Queue object.
  PyObject* key = PyLong_FromSize_t(self->index);
  if (!key) {
    return -1;
  }
  int rc = PyDict_SetItem(c->queues, key, self->queue);
  Py_DECREF(key);
  return rc;
}

static void Actor_dealloc(Actor* self) {
  if (self->ctx) {
    int closed = self->ctx->closed;
    if (self->actor) {
      // Destroying the actor is still valid after the context is closed (it
      // just no-ops against the torn-down runtime). The poller, however, has
      // been freed, so only unsubscribe while the context is still open.
      if (!closed) {
        mm_poller_unsubscribe(self->ctx->poller, self->index);
      }
      mm_actor_destroy(self->actor);
    }
    if (!closed) {
      PyObject* key = PyLong_FromSize_t(self->index);
      if (key) {
        if (PyDict_DelItem(self->ctx->queues, key) < 0) {
          PyErr_Clear();
        }
        Py_DECREF(key);
      }
    }
  }
  Py_XDECREF(self->queue);
  Py_XDECREF((PyObject*)self->ctx);
  Py_TYPE(self)->tp_free((PyObject*)self);
}

// Returns -1 (with an exception set) if the actor's context has been closed.
// Note: actor destruction is intentionally exempt and handled in Actor_dealloc.
static int actor_ensure_open(Actor* self) {
  if (!self->ctx || self->ctx->closed) {
    PyErr_SetString(PyExc_RuntimeError, "actor's context has been closed");
    return -1;
  }
  return 0;
}

static PyObject* Actor_send(Actor* self, PyObject* args) {
  PyObject* receiver;
  PyObject* parts_seq;
  if (actor_ensure_open(self) < 0) {
    return NULL;
  }
  if (!PyArg_ParseTuple(args, "OO", &receiver, &parts_seq)) {
    return NULL;
  }

  // Copy the receiver ident (bytes) first so a bad receiver fails before we
  // consume any payload bytearrays.
  mm_msg_part_t recv;
  if (bytes_to_part(receiver, &recv) < 0) {
    return NULL;
  }

  size_t n = 0;
  mm_msg_part_t* arr = build_parts(parts_seq, &n);
  if (!arr && PyErr_Occurred()) {
    if (recv.deleter) {
      recv.deleter(recv.deleter_ctx); // free the already-moved receiver
    }
    return NULL;
  }

  mm_msg_t msg = {.parts = arr, .n_parts = n};
  mm_err_t err = mm_actor_send(self->actor, recv, &msg);
  PyMem_Free(arr);
  if (err != MM_OK) {
    return set_mm_error(), NULL;
  }
  Py_RETURN_NONE;
}

static PyObject* Actor_next(Actor* self, PyObject* Py_UNUSED(ignored)) {
  if (actor_ensure_open(self) < 0) {
    return NULL;
  }

  // If nothing is already queued, busy-pump the poller for NEXT_SPIN_NS before
  // giving up and awaiting the fd. This catches a message that's about to
  // arrive on the cheap path and skips the eventfd round trip. Pumping also
  // routes messages for other actors into their queues. If the budget expires,
  // the final pump_drain has armed the poller, so the fd reader will still
  // deliver and wake the queue.get() below.
  //
  // asyncio.Queue has no __len__, so check emptiness via qsize().
  int empty = 1;
  PyObject* qs = PyObject_CallMethod(self->queue, "qsize", NULL);
  if (qs) {
    empty = (PyLong_AsLong(qs) == 0);
    Py_DECREF(qs);
  } else {
    PyErr_Clear();
  }
  if (empty) {
    struct timespec start;
    clock_gettime(CLOCK_MONOTONIC, &start);
    for (;;) {
      if (pump_drain(self->ctx, self->index)) {
        break; // a message for us was delivered
      }
      struct timespec now;
      clock_gettime(CLOCK_MONOTONIC, &now);
      long elapsed = (now.tv_sec - start.tv_sec) * 1000000000L +
          (now.tv_nsec - start.tv_nsec);
      if (elapsed >= NEXT_SPIN_NS) {
        break;
      }
    }
  }

  // queue.get() returns an awaitable that resolves to the next message —
  // immediately if one is queued, otherwise when the fd reader delivers one.
  return PyObject_CallMethod(self->queue, "get", NULL);
}

// Parse a role string ("parent" or "child") into mm_role_t. Returns -1 and
// sets an exception on an unknown value.
static int parse_role(const char* role, mm_role_t* out) {
  if (strcmp(role, "parent") == 0) {
    *out = MM_PARENT;
    return 0;
  }
  if (strcmp(role, "child") == 0) {
    *out = MM_CHILD;
    return 0;
  }
  PyErr_Format(
      PyExc_ValueError, "role must be 'parent' or 'child', not '%s'", role);
  return -1;
}

// Shared implementation for serve()/join().
static PyObject* actor_connect(
    Actor* self,
    const char* url,
    mm_role_t role,
    PyObject* name,
    PyObject* hello,
    PyObject* failure,
    int is_serve) {
  if (actor_ensure_open(self) < 0) {
    return NULL;
  }
  mm_connect_args_t args = {
      .role = role,
      .name_for_other = NULL,
      .hello_prefix = NULL,
      .failure_prefix = NULL,
  };
  mm_msg_part_t name_part;
  mm_msg_t hello_msg;
  mm_msg_t failure_msg;
  mm_msg_part_t* hello_arr = NULL;
  mm_msg_part_t* failure_arr = NULL;
  size_t hn = 0;
  size_t fn = 0;
  PyObject* result = NULL;

  if (name && name != Py_None) {
    if (bytes_to_part(name, &name_part) < 0) {
      return NULL;
    }
    args.name_for_other = &name_part;
  }
  if (hello && hello != Py_None) {
    hello_arr = build_parts(hello, &hn);
    if (!hello_arr && PyErr_Occurred()) {
      goto done;
    }
    hello_msg.parts = hello_arr;
    hello_msg.n_parts = hn;
    args.hello_prefix = &hello_msg;
  }
  if (failure && failure != Py_None) {
    failure_arr = build_parts(failure, &fn);
    if (!failure_arr && PyErr_Occurred()) {
      goto done;
    }
    failure_msg.parts = failure_arr;
    failure_msg.n_parts = fn;
    args.failure_prefix = &failure_msg;
  }

  mm_err_t err = is_serve ? mm_actor_serve(self->actor, url, &args)
                          : mm_actor_join(self->actor, url, &args);
  if (err != MM_OK) {
    set_mm_error();
    goto done;
  }
  Py_INCREF(Py_None);
  result = Py_None;

done:
  PyMem_Free(hello_arr);
  PyMem_Free(failure_arr);
  return result;
}

static PyObject* actor_connect_method(
    Actor* self,
    PyObject* args,
    PyObject* kwds,
    int is_serve) {
  static char* kw[] = {"url", "role", "name", "hello", "failure", NULL};
  const char* url;
  const char* role_str;
  PyObject* name = NULL;
  PyObject* hello = NULL;
  PyObject* failure = NULL;
  if (!PyArg_ParseTupleAndKeywords(
          args, kwds, "ss|OOO", kw, &url, &role_str, &name, &hello, &failure)) {
    return NULL;
  }
  mm_role_t role;
  if (parse_role(role_str, &role) < 0) {
    return NULL;
  }
  return actor_connect(self, url, role, name, hello, failure, is_serve);
}

static PyObject* Actor_serve(Actor* self, PyObject* args, PyObject* kwds) {
  return actor_connect_method(self, args, kwds, 1);
}

static PyObject* Actor_join(Actor* self, PyObject* args, PyObject* kwds) {
  return actor_connect_method(self, args, kwds, 0);
}

static PyObject* Actor_die(Actor* self, PyObject* args) {
  PyObject* reason;
  if (actor_ensure_open(self) < 0) {
    return NULL;
  }
  if (!PyArg_ParseTuple(args, "O", &reason)) {
    return NULL;
  }
  mm_msg_part_t p;
  if (bytes_to_part(reason, &p) < 0) {
    return NULL;
  }
  mm_actor_die(self->actor, p);
  Py_RETURN_NONE;
}

static PyObject* Actor_monitor(Actor* self, PyObject* args, PyObject* kwds) {
  static char* kw[] = {"ident", "failure", NULL};
  PyObject* ident;
  PyObject* failure = NULL;
  if (actor_ensure_open(self) < 0) {
    return NULL;
  }
  if (!PyArg_ParseTupleAndKeywords(args, kwds, "O|O", kw, &ident, &failure)) {
    return NULL;
  }

  mm_msg_part_t to_monitor;
  if (bytes_to_part(ident, &to_monitor) < 0) {
    return NULL;
  }
  mm_msg_part_t* arr = NULL;
  size_t n = 0;
  mm_msg_t failure_msg;
  mm_msg_t* failure_ptr = NULL;
  if (failure && failure != Py_None) {
    arr = build_parts(failure, &n);
    if (!arr && PyErr_Occurred()) {
      if (to_monitor.deleter) {
        to_monitor.deleter(to_monitor.deleter_ctx); // free the moved ident
      }
      return NULL;
    }
    failure_msg.parts = arr;
    failure_msg.n_parts = n;
    failure_ptr = &failure_msg;
  }

  mm_monitor_handle_t handle = NULL;
  mm_err_t err =
      mm_actor_monitor(self->actor, to_monitor, failure_ptr, &handle);
  PyMem_Free(arr);
  if (err != MM_OK) {
    return set_mm_error(), NULL;
  }

  MonitorHandle* mh = PyObject_New(MonitorHandle, &MonitorHandleType);
  if (!mh) {
    return NULL;
  }
  mh->handle = handle;
  return (PyObject*)mh;
}

static PyMethodDef Actor_methods[] = {
    {"send",
     (PyCFunction)Actor_send,
     METH_VARARGS,
     "send(receiver: bytes, parts: list[bytes]) -> None"},
    {"next",
     (PyCFunction)Actor_next,
     METH_NOARGS,
     "next() -> awaitable[list[bytes]]: next delivered message"},
    {"serve",
     (PyCFunction)Actor_serve,
     METH_VARARGS | METH_KEYWORDS,
     "serve(url, role, name=None, hello=None, failure=None) -> None; "
     "role is 'parent' or 'child'"},
    {"join",
     (PyCFunction)Actor_join,
     METH_VARARGS | METH_KEYWORDS,
     "join(url, role, name=None, hello=None, failure=None) -> None; "
     "role is 'parent' or 'child'"},
    {"die", (PyCFunction)Actor_die, METH_VARARGS, "die(reason: bytes) -> None"},
    {"monitor",
     (PyCFunction)Actor_monitor,
     METH_VARARGS | METH_KEYWORDS,
     "monitor(ident: bytes, failure=None) -> MonitorHandle"},
    {NULL, NULL, 0, NULL},
};

static PyTypeObject ActorType = {
    PyVarObject_HEAD_INIT(NULL, 0).tp_name = "minimonarch.Actor",
    .tp_basicsize = sizeof(Actor),
    .tp_dealloc = (destructor)Actor_dealloc,
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_doc = "A minimonarch actor bound to the current context's poller.",
    .tp_methods = Actor_methods,
    .tp_init = (initproc)Actor_init,
    .tp_new = PyType_GenericNew,
};

// ---------------------------------------------------------------------------
// MonitorHandle
// ---------------------------------------------------------------------------

static PyObject* MonitorHandle_cancel(
    MonitorHandle* self,
    PyObject* Py_UNUSED(ignored)) {
  if (self->handle) {
    mm_monitor_handle_cancel(self->handle);
    self->handle = NULL;
  }
  Py_RETURN_NONE;
}

static void MonitorHandle_dealloc(MonitorHandle* self) {
  Py_TYPE(self)->tp_free((PyObject*)self);
}

static PyMethodDef MonitorHandle_methods[] = {
    {"cancel",
     (PyCFunction)MonitorHandle_cancel,
     METH_NOARGS,
     "cancel() -> None: stop delivering the failure message"},
    {NULL, NULL, 0, NULL},
};

static PyTypeObject MonitorHandleType = {
    PyVarObject_HEAD_INIT(NULL, 0).tp_name = "minimonarch.MonitorHandle",
    .tp_basicsize = sizeof(MonitorHandle),
    .tp_dealloc = (destructor)MonitorHandle_dealloc,
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_doc = "Handle for a registered monitor; call cancel() to deregister.",
    .tp_methods = MonitorHandle_methods,
};

// ---------------------------------------------------------------------------
// Module
// ---------------------------------------------------------------------------

static PyMethodDef pump_def = {"_mm_pump", (PyCFunction)mm_pump, METH_O, NULL};

// close() -> bool: tear down the minimonarch context installed in the current
// contextvars context, if any. Destroys its actors, poller, and the underlying
// runtime (mm_ctx_destroy), and resets the contextvar so the next Actor()
// creates a fresh context. Returns True if a context was closed, else False.
static PyObject* mm_close(PyObject* self, PyObject* Py_UNUSED(ignored)) {
  PyObject* cur = NULL;
  if (PyContextVar_Get(g_ctx_var, NULL, &cur) < 0) {
    return NULL;
  }
  if (cur == NULL) {
    Py_RETURN_FALSE; // no context in this contextvars context
  }
  Context* c = (Context*)cur; // new ref

  context_shutdown(c);

  if (c->token) {
    int rc = PyContextVar_Reset(g_ctx_var, c->token);
    Py_CLEAR(c->token);
    if (rc < 0) {
      Py_DECREF(c);
      return NULL;
    }
  }
  Py_DECREF(c);
  Py_RETURN_TRUE;
}

static PyMethodDef module_methods[] = {
    {"close",
     (PyCFunction)mm_close,
     METH_NOARGS,
     "close() -> bool: destroy the current context's minimonarch runtime"},
    {NULL, NULL, 0, NULL},
};

static struct PyModuleDef minimonarch_module = {
    PyModuleDef_HEAD_INIT,
    .m_name = "minimonarch",
    .m_doc = "asyncio bindings for the minimonarch C API",
    .m_size = -1,
    .m_methods = module_methods,
};

static int import_callable(const char* mod, const char* attr, PyObject** out) {
  PyObject* m = PyImport_ImportModule(mod);
  if (!m) {
    return -1;
  }
  *out = PyObject_GetAttrString(m, attr);
  Py_DECREF(m);
  return *out ? 0 : -1;
}

PyMODINIT_FUNC PyInit_minimonarch(void) {
  if (PyType_Ready(&ContextType) < 0 || PyType_Ready(&ActorType) < 0 ||
      PyType_Ready(&MonitorHandleType) < 0 ||
      PyType_Ready(&ByteArrayType) < 0) {
    return NULL;
  }

  if (import_callable("asyncio", "get_running_loop", &g_get_running_loop) < 0) {
    return NULL;
  }
  if (import_callable("asyncio", "Queue", &g_queue_type) < 0) {
    return NULL;
  }

  g_ctx_var = PyContextVar_New("minimonarch.context", NULL);
  if (!g_ctx_var) {
    return NULL;
  }
  g_pump_func = PyCFunction_New(&pump_def, NULL);
  if (!g_pump_func) {
    return NULL;
  }

  PyObject* m = PyModule_Create(&minimonarch_module);
  if (!m) {
    return NULL;
  }

  Py_INCREF(&ActorType);
  Py_INCREF(&MonitorHandleType);
  Py_INCREF(&ByteArrayType);
  if (PyModule_AddObject(m, "Actor", (PyObject*)&ActorType) < 0 ||
      PyModule_AddObject(m, "MonitorHandle", (PyObject*)&MonitorHandleType) <
          0 ||
      PyModule_AddObject(m, "bytearray", (PyObject*)&ByteArrayType) < 0) {
    Py_DECREF(m);
    return NULL;
  }
  return m;
}
