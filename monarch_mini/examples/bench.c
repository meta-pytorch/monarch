/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

/*
 * bench.c — minimal in-process self send->receive round-trip latency.
 * Sends a small message to self, waits for it on the poller fd, repeats.
 */

#include <poll.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "minimonarch.h"

#define CHECK(call)                                               \
  do {                                                            \
    mm_err_t _e = (call);                                         \
    if (_e != MM_OK) {                                            \
      fprintf(stderr, "%s failed: %s\n", #call, mm_last_error()); \
      exit(1);                                                    \
    }                                                             \
  } while (0)

static const char IDENT[] = "bench@root";

static double now_s(void) {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return (double)ts.tv_sec + (double)ts.tv_nsec * 1e-9;
}

int main(int argc, char** argv) {
  int iters = argc > 1 ? atoi(argv[1]) : 1000;

  mm_ctx_t ctx = NULL;
  CHECK(mm_ctx_create(&ctx));
  mm_msg_part_t ident = {
      .data = IDENT,
      .len = strlen(IDENT),
      .deleter = NULL,
      .deleter_ctx = NULL};
  mm_actor_t actor = NULL;
  CHECK(mm_actor_create(ctx, &ident, &actor));
  int fd = -1;
  mm_poller_t poller = NULL;
  CHECK(mm_poller_create(ctx, &fd, &poller));
  CHECK(mm_poller_subscribe(poller, 0, actor));

  char payload[64] = {0};
  mm_msg_part_t part = {
      .data = payload,
      .len = sizeof(payload),
      .deleter = NULL,
      .deleter_ctx = NULL};
  mm_msg_t msg = {.parts = &part, .n_parts = 1};
  mm_msg_part_t receiver = {
      .data = IDENT,
      .len = strlen(IDENT),
      .deleter = NULL,
      .deleter_ctx = NULL};

  size_t index = 0;
  mm_msg_part_t parts[4];
  size_t n = 0;

  // (A) Latency, poll-based: ping-pong, blocking on the poller fd (eventfd).
  double start = now_s();
  for (int i = 0; i < iters; i++) {
    CHECK(mm_actor_send(actor, receiver, &msg));
    for (;;) {
      mm_err_t e = mm_poller_next(poller, &index, parts, 4, &n);
      if (e == MM_OK) {
        break;
      }
      if (e != MM_ENOMSG) {
        fprintf(stderr, "next failed: %s\n", mm_last_error());
        exit(1);
      }
      struct pollfd pfd = {.fd = fd, .events = POLLIN};
      poll(&pfd, 1, -1);
    }
  }
  double lat_poll = (now_s() - start) / iters;

  // (A') Latency, busy-poll: ping-pong, spinning on mm_poller_next, never
  // touching the eventfd/poll. Relies on the arm-dedup so the spin doesn't
  // flood ArmPoller commands.
  start = now_s();
  for (int i = 0; i < iters; i++) {
    CHECK(mm_actor_send(actor, receiver, &msg));
    while (mm_poller_next(poller, &index, parts, 4, &n) != MM_OK) {
      /* spin */
    }
  }
  double lat_busy = (now_s() - start) / iters;

  // (B) Throughput: send all, then drain all. Amortizes the cross-thread
  // wakeup / eventfd / poll cost over many messages.
  start = now_s();
  for (int i = 0; i < iters; i++) {
    CHECK(mm_actor_send(actor, receiver, &msg));
  }
  for (int got = 0; got < iters;) {
    mm_err_t e = mm_poller_next(poller, &index, parts, 4, &n);
    if (e == MM_OK) {
      got++;
    } else if (e == MM_ENOMSG) {
      struct pollfd pfd = {.fd = fd, .events = POLLIN};
      poll(&pfd, 1, -1);
    } else {
      fprintf(stderr, "next failed: %s\n", mm_last_error());
      exit(1);
    }
  }
  double thr = (now_s() - start) / iters;

  printf("%d iters, 64 B:\n", iters);
  printf("  latency (poll/eventfd): %.3f us/round-trip\n", lat_poll * 1e6);
  printf("  latency (busy-poll):    %.3f us/round-trip\n", lat_busy * 1e6);
  printf("  throughput (batched):   %.3f us/msg\n", thr * 1e6);

  mm_poller_unsubscribe(poller, 0);
  mm_poller_destroy(poller);
  mm_actor_destroy(actor);
  mm_ctx_destroy(ctx);
  return 0;
}
