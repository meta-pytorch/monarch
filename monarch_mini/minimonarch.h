/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

/*
 * minimonarch.h — Miniture Monarch C API
 * This is an attempt at defining what the most minimal "OS"-level API would
 * be on which we can build the higher level monarch primitives such that
 * the hard stuff (message routing, failure of connection detection delivery,
 * monitoring) is handled, and lifetime management is tractable.
 *
 * This API is expressed here in C to illustrate how this can be integrated into
 * all languages (Rust, Python, C++)
 *
 * The core idea is to try to make everything an actor including connection
 * establishment, so that all errors are handled consistently.
 */

#pragma once

#include <stdbool.h>
#include <stddef.h>

/* ---------------------------------------------------------------------------
 * Types
 * ---------------------------------------------------------------------------
 *
 * Messages are multipart: each part is a sized byte buffer plus a deleter.
 * This design, copied from zmq, allows this layer to make as few serialization
 * decisions as possible while still allowing for message headers and zero-copy
 * payload handling.
 */

/* A single part of a multipart message. */
typedef struct mm_msg_part {
  const void* data;
  size_t len;
  /* Called with `ctx` when this part's memory may be freed. May be NULL. */
  void (*deleter)(void* ctx);
  void* deleter_ctx;
} mm_msg_part_t;

/* A complete multipart message: an array of `n_parts` parts. */
typedef struct mm_msg {
  mm_msg_part_t* parts;
  size_t n_parts;
} mm_msg_t;

/* Opaque handle types. */
typedef struct mm_ctx* mm_ctx_t;
typedef struct mm_actor* mm_actor_t;
typedef struct mm_monitor_handle* mm_monitor_handle_t;
typedef struct mm_poller* mm_poller_t;

typedef enum mm_role {
  MM_CHILD = 0,
  MM_PARENT = 1,
} mm_role_t;

/* Arguments shared by mm_actor_serve and mm_actor_join. */
typedef struct mm_connect_args {
  mm_role_t role; /* MM_PARENT or MM_CHILD */
  mm_msg_part_t*
      name_for_other; /* optional: assign a name to the remote actor */
  const mm_msg_t*
      hello_prefix; /* prefix for the connection-established message */
  const mm_msg_t* failure_prefix; /* prefix for connection-failure messages */
} mm_connect_args_t;

typedef enum mm_err {
  MM_OK = 0, /* success */
  MM_ENOMSG = 1, /* no message available (mm_poller_next) */
  MM_EBUFSZ = -1, /* parts buffer too small; n_parts_out holds required size */
  MM_EINTERNAL = -2, /* internal error */
} mm_err_t;

/* -- Function Summary
 * ---------------------------------------------------------------- */
const char* mm_last_error(void);
mm_err_t mm_ctx_create(mm_ctx_t* out);
void mm_ctx_destroy(mm_ctx_t ctx);
mm_err_t mm_actor_create(
    mm_ctx_t ctx,
    mm_msg_part_t* ident,
    bool gateway,
    mm_actor_t* out);
void mm_actor_destroy(mm_actor_t actor);
mm_err_t mm_actor_send(
    mm_actor_t actor,
    mm_msg_part_t receiver_ident,
    const mm_msg_t* msg);
mm_err_t mm_actor_serve(
    mm_actor_t actor,
    const char* url,
    const mm_connect_args_t* args);
mm_err_t
mm_actor_join(mm_actor_t actor, const char* url, const mm_connect_args_t* args);
void mm_actor_die(mm_actor_t actor, mm_msg_part_t reason);
mm_err_t mm_actor_monitor(
    mm_actor_t actor,
    mm_msg_part_t to_monitor_ident,
    const mm_msg_t* failure_prefix,
    mm_monitor_handle_t* out);
void mm_monitor_handle_cancel(mm_monitor_handle_t handle);
mm_err_t mm_poller_create(mm_ctx_t ctx, int* fd_out, mm_poller_t* out);
void mm_poller_destroy(mm_poller_t poller);
mm_err_t
mm_poller_subscribe(mm_poller_t poller, size_t index, mm_actor_t actor);
void mm_poller_unsubscribe(mm_poller_t poller, size_t index);
mm_err_t mm_poller_next(
    mm_poller_t poller,
    size_t* index_out,
    mm_msg_part_t* parts,
    size_t parts_cap,
    size_t* n_parts_out);
/* ---------------------------------------------------------------------------
 */

/* ---------------------------------------------------------------------------
 * Error reporting
 * ---------------------------------------------------------------------------
 */

/*
 * Return a human-readable description of the last error that occurred on this
 * thread. The returned pointer is valid until the next mm_* call on this
 * thread. Returns an empty string if no error has occurred yet.
 */
const char* mm_last_error(void);

/* ---------------------------------------------------------------------------
 * Context
 * ---------------------------------------------------------------------------
 *
 * A context holds the runtime state shared across all actors created within it:
 * the internal event loop, connection state, and routing tables.
 * All actors and pollers are created from a context.
 */

/*
 * Create a new context, writing it to `*out`.
 * The caller must eventually call mm_ctx_destroy().
 */
mm_err_t mm_ctx_create(mm_ctx_t* out);

/*
 * Destroy a context and release all associated resources.
 * All actors and pollers created from this context must be destroyed first.
 */
void mm_ctx_destroy(mm_ctx_t ctx);

/* ---------------------------------------------------------------------------
 * Actor
 * ---------------------------------------------------------------------------
 *
 * An addressable endpoint for delivering messages.
 */

/*
 * Create a new actor, writing it to `*out`.
 *
 * `ident` must be unique across the whole monarch execution. `ident` are
 * formatted similar to email addresses: `uuid@endpoint`. The expectation is
 * that a client uses a UUID or some other scheme that will create unique actor
 * names across the run. The @endpoint is the address of the 'endpoint' TCP
 * server that owns the actors. See Message Routing Below. The 'endpoint' may be
 * omitted if the actor is a parent of the sending actor. Naming the actor on
 * creation is optional. If the name is not specified, it must be provided in
 * the join call. The caller must eventually call mm_actor_destroy().
 *
 * `gateway` declares whether this actor is a gateway: the entry point for its
 * process group. A gateway must have no parent or a network (tcp/quic) parent;
 * joining a gateway to a unix:// or inproc:// parent is rejected (delivered as
 * a failure message). The flag is fixed at creation and never changes. Pass
 * true for a root/endpoint actor that owns a machine-local subtree, false for
 * an ordinary actor that will join a local parent.
 */
mm_err_t mm_actor_create(
    mm_ctx_t ctx,
    mm_msg_part_t* ident,
    bool gateway,
    mm_actor_t* out);

/*
 * Destroy an actor and release all associated resources.
 */
void mm_actor_destroy(mm_actor_t actor);

/*
 * Send a multipart message to `receiver_ident`.
 *
 * An actor can send to any receiver_ident that is connected to the job.
 * The actor does not have to be directly joined to this actor.
 * The receiver_endpoint is the join/serve URL that is consider the 'endpoint'
 * for the receivering actor. This is the 'closest' TCP server for reaching the
 * actor, or 'ancestor://' for messaging a direct ancestor of this actor. See
 * 'Message Routing' below for details.
 */
mm_err_t mm_actor_send(
    mm_actor_t actor,
    mm_msg_part_t receiver_ident,
    const mm_msg_t* msg);

/*
 * Serve/Join — Establishing connections
 * ======================================
 *
 * serve and join take identical arguments so that both sides of a connection
 * agree on the topology: who becomes parent, who becomes child, and what names
 * are assigned. Either side can be the server (listen) or the joiner (connect);
 * the parent/child relationship is negotiated independently via `adopt`.
 *
 * url formats:
 *   TCP:         "tcp://192.168.0.101:8000"
 *   UNIX socket: "unix:///tmp/local_socket"
 *
 * role:
 *   MM_PARENT — this actor becomes the parent of the remote actor.
 *   MM_CHILD  — this actor becomes the child of the remote actor.
 *   Both sides must agree: exactly one side must pass MM_PARENT.
 *
 * name_for_other:
 *   Optional. If the remote actor was created without an ident (NULL passed to
 *   mm_actor_create), this side may supply its name here. This lets the parent
 *   assign a canonical name to a child it is spawning, or let a well-known
 *   server be named by its first connecting client.
 *   Pass NULL if the remote actor already has a name or if you do not wish to
 *   assign one.
 *
 * Naming and topology errors (mismatched adopt flags, name conflicts,
 * attempting to add a child before a parent is established) are not returned
 * immediately. They are delivered asynchronously as failure messages using
 * failure_prefix.
 *
 * An actor may have many children but only one parent. An actor must establish
 * its parent connection before accepting any children. These rules are enforced
 * dynamically; violations produce failure messages rather than synchronous
 * errors. Enforcing a single parent and requiring the parent to be established
 * first keeps the implementation tractable:
 *   1. Cycles in the parent graph are impossible.
 *   2. The full ancestry path to any child is known before messages can be
 *      delivered to it, enabling endpoint-based routing.
 *
 * A join consumes a single pending serve. To accept multiple joiners, call
 * serve again after each join. Serves on the same url are queued and paired
 * with joiners one at a time; the socket stays open between pairs.
 *
 * On successful connection, this actor receives:
 *   [hello_prefix..., self_ident, other_ident]
 *
 * On connection failure or disconnect, this actor receives:
 *   [failure_prefix..., other_ident, reason]
 *
 * It is not necessary for the serve to be posted before the join; the joiner
 * will retry until the port is ready (similar to zmq).
 */
mm_err_t mm_actor_serve(
    mm_actor_t actor,
    const char* url,
    const mm_connect_args_t* args);

mm_err_t
mm_actor_join(mm_actor_t actor, const char* url, const mm_connect_args_t* args);

/*
 * Signal that this actor is now dead to all monitors, parents, and children.
 *
 * No new messages will be delivered after this call. Messages already delivered
 * to a poller may still be returned by mm_poller_next().
 *
 * `reason` is a utf-8 string explaining why the actor died.
 */
void mm_actor_die(mm_actor_t actor, mm_msg_part_t reason);

/*
 * Monitor another actor.
 *
 * If `to_monitor_ident` dies or is already dead, this actor will be sent:
 *   [failure_prefix[0], ..., failure_prefix[n_failure-1], to_monitor_ident,
 * reason]
 *
 * On success, writes the new handle to `*out`.
 *
 * Ports
 * -----
 * Using multipart messages means that things like ports can just be built
 * using headers. For instance, if you want a port to monitor an actor who is
 * sending a reply, you can call monitor but pass it a failure_prefix that will
 * route to however you chose to encode ports in the message framework. Hence
 * we prevent needing additional "OS"-level concepts, keeping everything an
 * actor.
 */
mm_err_t mm_actor_monitor(
    mm_actor_t actor,
    mm_msg_part_t to_monitor_ident,
    const mm_msg_t* failure_prefix,
    mm_monitor_handle_t* out);

/* ---------------------------------------------------------------------------
 * MonitorHandle
 * ---------------------------------------------------------------------------
 */

/*
 * Cancel the monitor.
 *
 * The failure message will no longer be delivered after this call. If there
 * is one still undelivered in the internal queue it will be dropped before
 * delivery on mm_poller_next().
 */
void mm_monitor_handle_cancel(mm_monitor_handle_t handle);

/* ---------------------------------------------------------------------------
 * Poller
 * ---------------------------------------------------------------------------
 *
 * Used to integrate actor messaging into the surrounding event loop.
 * The Poller exposes a wakeup file descriptor that becomes readable when more
 * delivered messages may exist, allowing monarch messaging to be driven by any
 * fd-based event loop (select, poll, epoll, kqueue, etc.).
 *
 * Calls on a single poller must be externally serialized.
 */

/*
 * Create a new Poller from `ctx`, writing its handle to `*out`.
 *
 * If `fd_out` is non-NULL, writes the poller's wakeup file descriptor to
 * `*fd_out`. The fd is suitable for use with select/poll/epoll. Callers should
 * first drain mm_poller_next() until it returns MM_ENOMSG, then wait on this
 * fd. After the fd becomes readable, call mm_poller_next() again.
 *
 * The returned fd is owned by the poller; do not close it.
 *
 * The caller must eventually call mm_poller_destroy().
 */
mm_err_t mm_poller_create(mm_ctx_t ctx, int* fd_out, mm_poller_t* out);

/*
 * Destroy a Poller and free all associated resources.
 */
void mm_poller_destroy(mm_poller_t poller);

/*
 * Watch `actor` for incoming messages, associating it with `index`.
 * An actor may be subscribed to at most one poller at a time.
 *
 * Example:
 *   for (int i = 0; i < n; i++)
 *       mm_poller_subscribe(p, i, actors[i]);
 */
mm_err_t
mm_poller_subscribe(mm_poller_t poller, size_t index, mm_actor_t actor);

/*
 * Stop watching the actor previously assigned `index`.
 */
void mm_poller_unsubscribe(mm_poller_t poller, size_t index);

/*
 * Read the next delivered message into `parts[0..parts_cap-1]`, writing the
 * registered actor index to `*index_out`. Returns MM_ENOMSG if no message is
 * available. Never blocks.
 *
 * On success: `*n_parts_out` parts are written to `parts` and the message is
 * consumed.
 *
 * On MM_EBUFSZ: `*n_parts_out` is set to the number of parts required; no
 * parts are written and the message is NOT consumed. The caller can retry with
 * a buffer of at least `*n_parts_out` entries.
 *
 * Example:
 *   size_t id;
 *   mm_msg_part_t parts[8];
 *   size_t n;
 *   while (mm_poller_next(p, &id, parts, 8, &n) == MM_OK) {
 *       // handle parts[0..n-1] ...
 *   }
 */
mm_err_t mm_poller_next(
    mm_poller_t poller,
    size_t* index_out,
    mm_msg_part_t* parts,
    size_t parts_cap,
    size_t* n_parts_out);

/* ---------------------------------------------------------------------------
 * Message Routing
 * ---------------------------------------------------------------------------
 *
 * Messages are not limited to travelling along the directly-joined parent-child
 * links. Each actor has an endpoint: the address of the closest tcp:// server
 * in its parent hierarchy. Actor idents are formatted as `uuid@endpoint`, so
 * the routing address is encoded in the name — no global DNS lookup is needed.
 *
 * A message sent to `uuid@endpoint` is delivered directly to that endpoint,
 * which then forwards it to the local actor over intra-machine parent-child
 * links. For actors on the same machine as the root (no tcp:// ancestor), the
 * message travels up the parent-child links to the root and is delivered there.
 *
 * Zero Copy
 * ---------
 * Messages are lists of (pointer, length, deleter) segments. Forwarding a
 * message passes the pointer without copying; the deleter is called once the
 * segment is no longer needed. This allows large payloads to transit multiple
 * hops without any copies.
 *
 * Scale
 * -----
 * tcp:// endpoints support large numbers of children. Message routing already
 * scales because each send goes directly to the receiver's endpoint. Liveness
 * monitoring for large TCP fanout is handled internally; callers observe the
 * same failure-message semantics regardless of fanout size.
 * See IMPLEMENTATION.md for details.
 */
