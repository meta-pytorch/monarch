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
typedef struct mm_actor* mm_actor_t;
typedef struct mm_monitor_handle* mm_monitor_handle_t;
typedef struct mm_poller* mm_poller_t;

typedef enum mm_err {
  MM_OK = 0, /* success */
  MM_ENOMSG = 1, /* no message available (mm_actor_next, mm_poller_next) */
  MM_EBUFSZ = -1, /* parts buffer too small; n_parts_out holds required size */
  MM_EINTERNAL = -2, /* internal error */
} mm_err_t;

/* -- Function Summary
 * ---------------------------------------------------------------- */
mm_err_t mm_actor_create(mm_msg_part_t ident, mm_actor_t* out);
void mm_actor_destroy(mm_actor_t actor);
mm_err_t mm_actor_send(
    mm_actor_t actor,
    mm_msg_part_t receiver_ident,
    const mm_msg_t* msg);
mm_err_t mm_actor_next(
    mm_actor_t actor,
    mm_msg_part_t* parts,
    size_t parts_cap,
    size_t* n_parts_out);
mm_err_t mm_actor_serve(
    mm_actor_t actor,
    const char* url,
    const mm_msg_t* hello_prefix,
    const mm_msg_t* failure_prefix);
mm_err_t mm_actor_join(
    mm_actor_t actor,
    const char* url,
    int adopt,
    const mm_msg_t* hello_prefix,
    const mm_msg_t* failure_prefix);
void mm_actor_die(mm_actor_t actor, mm_msg_part_t reason);
mm_err_t mm_actor_monitor(
    mm_actor_t actor,
    mm_msg_part_t to_monitor_ident,
    const mm_msg_t* failure_prefix,
    mm_monitor_handle_t* out);

void mm_monitor_handle_cancel(mm_monitor_handle_t handle);

mm_err_t mm_poller_create(mm_poller_t* out);
void mm_poller_destroy(mm_poller_t poller);
mm_err_t
mm_poller_subscribe(mm_poller_t poller, size_t index, mm_actor_t actor);
void mm_poller_unsubscribe(mm_poller_t poller, size_t index);
mm_err_t mm_poller_next(mm_poller_t poller, size_t* index_out);
mm_err_t mm_poller_fd(mm_poller_t poller, int* fd_out);
/* ---------------------------------------------------------------------------
 */

/* ---------------------------------------------------------------------------
 * Actor
 * ---------------------------------------------------------------------------
 *
 * An addressable endpoint for delivering messages.
 */

/*
 * Create a new actor.
 *
 * `ident` must be unique across the whole monarch execution. We take it as an
 * input so that it is possible to generate references to actors before they are
 * actually created, even when actor creation is done by other actors.
 *
 * The expectation is that a client uses a UUID or some other scheme that will
 * create unique actor names across the run.
 *
 * On success, writes the new actor to `*out`.
 * The caller must eventually call mm_actor_destroy().
 */
mm_err_t mm_actor_create(mm_msg_part_t ident, mm_actor_t* out);

/*
 * Destroy an actor and release all associated resources.
 */
void mm_actor_destroy(mm_actor_t actor);

/*
 * Send a multipart message to `receiver_ident`.
 *
 * An actor can send to any receiver_ident that is connected to the job.
 * The actor does not have to be directly joined to this actor.
 * See 'Message Routing' below.
 */
mm_err_t mm_actor_send(
    mm_actor_t actor,
    mm_msg_part_t receiver_ident,
    const mm_msg_t* msg);

/*
 * Read the next message sent to this actor into `parts[0..parts_cap-1]`.
 *
 * Does not block. Returns MM_OK on success, MM_ENOMSG if no message is
 * available, MM_EBUFSZ if the buffer is too small, or another mm_err_t.
 *
 * On success: `*n_parts_out` parts are written to `parts` and the message is
 * consumed.
 *
 * On MM_EBUFSZ: `*n_parts_out` is set to the number of parts required; no
 * parts are written and the message is NOT consumed. The caller can retry with
 * a buffer of at least `*n_parts_out` entries.
 *
 * Because the caller supplies the buffer, no separate free call is needed.
 * A stack-allocated or cached array of a few parts handles the common case:
 *
 *   mm_msg_part_t parts[8];
 *   size_t n;
 *   mm_err_t err = mm_actor_next(actor, parts, 8, &n);
 *   if (err == MM_EBUFSZ) {
 *       mm_msg_part_t *big = malloc(n * sizeof(*big));
 *       err = mm_actor_next(actor, big, n, &n);
 *       // ... handle ...
 *       free(big);
 *   }
 */
mm_err_t mm_actor_next(
    mm_actor_t actor,
    mm_msg_part_t* parts,
    size_t parts_cap,
    size_t* n_parts_out);

/*
 * Serve this actor on the connection described by `url`.
 *
 * When another actor joins the url it becomes a child or parent of this actor.
 *
 * url formats:
 *   TCP:         "tcp://192.168.0.101:8000"
 *   UNIX socket: "unix:///tmp/local_socket"
 *
 * A join consumes the serve. If you want to allow multiple joiners, call serve
 * again after each join. Serves on the same url are serialized awaiting joins
 * so you can, for instance, list multiple actors served on the url and they
 * will be handed out one at a time. Once we get the first serve, we will keep
 * the socket open waiting for more joins and pairing them to serves.
 *
 * When an actor joins, this actor will be sent:
 *   [hello_prefix[0], ..., hello_prefix[n_hello-1], actor_ident]
 *
 * If a joiner of this socket fails or the connection breaks, this actor will
 * be sent:
 *   [failure_prefix[0], ..., failure_prefix[n_failure-1], actor_ident, reason]
 *
 * Failures and connects are just another kind of message. hello_prefix and
 * failure_prefix let the consuming event loop choose how to encode them.
 */
mm_err_t mm_actor_serve(
    mm_actor_t actor,
    const char* url,
    const mm_msg_t* hello_prefix,
    const mm_msg_t* failure_prefix);

/*
 * Connect this actor to the actor served at `url`.
 *
 * Join/Serve Directionality
 * -------------------------
 * By keeping the server and the joiner separate from who becomes parent and
 * child, we make it possible to implement many patterns of establishing a
 * network. For instance, worker processes will serve host actors on a TCP
 * connection with the client joining to the hosts as a parent. Whereas host
 * actors will serve a UNIX socket on which proc actors join as children.
 *
 * If `adopt` is true, this actor becomes the parent; otherwise it becomes the
 * child.
 *
 * When the join succeeds, this actor will be sent:
 *   [hello_prefix[0], ..., hello_prefix[n_hello-1], actor_ident]
 *
 * It is not necessary for the serve to have started before the join. Similar
 * to zmq we will retry waiting for the port to become ready.
 *
 * If the serving actor fails or the connection breaks, this actor will be sent:
 *   [failure_prefix[0], ..., failure_prefix[n_failure-1], parent_ident, reason]
 *
 * Actors may have multiple parents and multiple children. If an actor has 1 or
 * more parents, there must exist one parent that is itself not a descendant of
 * that actor. If a failure occurs such that an actor is left with parents who
 * are its descendants, it will deliver failure messages for those actors to
 * the original actor. (IOW there must be a DAG that goes back to the root
 * actor.)
 */
mm_err_t mm_actor_join(
    mm_actor_t actor,
    const char* url,
    int adopt,
    const mm_msg_t* hello_prefix,
    const mm_msg_t* failure_prefix);

/*
 * Signal that this actor is now dead to all monitors, parents, and children.
 *
 * No new messages will be delivered after this call. mm_actor_next() will
 * still return messages sent prior to calling mm_actor_die(), and messages
 * can still be routed over connections that are still alive in order to clean
 * up.
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
 * delivery on mm_actor_next().
 */
void mm_monitor_handle_cancel(mm_monitor_handle_t handle);

/* ---------------------------------------------------------------------------
 * Poller
 * ---------------------------------------------------------------------------
 *
 * Used to integrate actor messaging into the surrounding event loop.
 *
 * Event Loops
 * -----------
 * There is no built-in event loop. Instead the Poller interface provides a
 * way to add monarch messaging to existing loops. The implementation
 * internally will likely use its own event loop, for instance powered by a
 * single-thread tokio loop. This protects this lower-level loop from
 * interference from Python deadlocks or GC. Since keepalive timeouts live in
 * this protected loop, there is less chance of them being false positives.
 */

/*
 * Create a new Poller, writing it to `*out`.
 * The caller must eventually call mm_poller_destroy().
 */
mm_err_t mm_poller_create(mm_poller_t* out);

/*
 * Destroy a Poller and free all associated resources.
 */
void mm_poller_destroy(mm_poller_t poller);

/*
 * Watch `actor` for incoming messages, associating it with `index`.
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
 * If any subscribed actor has a message available, write its index to
 * `*index_out`. Returns MM_ENOMSG if no actor has a message available.
 * Never blocks.
 *
 * Example:
 *   size_t id;
 *   mm_msg_part_t parts[8];
 *   size_t n;
 *   while (mm_poller_next(p, &id) == MM_OK) {
 *       mm_actor_next(actors[id], parts, 8, &n);
 *       // handle parts[0..n-1] ...
 *   }
 */
mm_err_t mm_poller_next(mm_poller_t poller, size_t* index_out);

/*
 * Write the file descriptor that becomes readable when mm_poller_next() would
 * return MM_OK to `*fd_out`. Suitable for use with select/poll/epoll.
 *
 * The returned fd is owned by the poller; do not close it.
 */
mm_err_t mm_poller_fd(mm_poller_t poller, int* fd_out);

/* ---------------------------------------------------------------------------
 * Message Routing
 * ---------------------------------------------------------------------------
 *
 * While the connection topology is established via serve/join pairs, messages
 * are not forced to go between the actors that are explicitly joined. Instead
 * messages are routed via a simple algorithm. For the client/host/proc setup
 * described above, this algorithm coincides with how messages are routed today:
 *
 *   - For the receiver, walk up the parent-child links until you find a
 *     tcp:// url used as the link. Call the server-side actor the receiving
 *     actor (RA).
 *   - Walk up the sender-side links until you find a tcp:// url; call the
 *     closer ancestor of this link the sending actor (SA).
 *   - The SA will establish and cache a connection to the RA. Messages will be
 *     routed through the non-tcp links, and then across this tcp link to reach
 *     the receiver.
 *   - If this connection cannot be established, we continue searching up the
 *     parent-child links looking for a new SA/RA pair.
 *   - If SA==RA, then the connection is implicitly established through existing
 *     links, and we fall back to routing the messages through this shared
 *     ancestor actor.
 *
 * We do not expect this algorithm to work naively at scale. Instead, we expect
 * that the next level of abstraction will use comm actors to distribute
messages
 * more intelligently knowing how this layer will route everything.
 *
 * To establish a connection, we have to figure out the ancestry of the
receiving
 * actor. This can probably be implemented like a hierarchical DNS where the
 * system queries the primary parent for the information and caches the results.
 * It will have to be updated on failures.
 *
 *
 * Zero Copy
 * ---------
 * The messages are lists of bytes, but expressed as a list of sized segments
 * plus deleters. This lets us do schemes where we attach headers or move
 * memory between processes without copies.

* Parent DAG
* ----------
*
* To create redundancy, we allow an actor to have multiple parents. Its primary
* parent starts as the first parent joined, and is how we compute its ancestry.
It
* can have multiple secondary parents. If the primary fails, it will fail over
to
* the secondary and can continue to send messages. It can then add another
parent
* later. The intent is that that we form comm actors into fat trees so that we
* have a built in degree of individual machine redundancy. We have to be careful
* about weird possibilities such as cycles. My proposal is to discover the
cycles
* only when the failover happens and then just consider the cyclic parent dead
as
* well if we detect it on failover. However there are many other ways -- forcing
* each actor to have a generation vector clock, or even just expecting the
* wrappers will prevent this.
*/
