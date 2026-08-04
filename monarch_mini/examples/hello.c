/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

/*
 * hello.c — minimonarch hello world.
 * Creates an actor, sends a message to itself, then receives it back.
 * All calls return MM_EINTERNAL until the API is implemented.
 */

#include <poll.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "minimonarch.h"

#define CHECK(call)                                               \
  do {                                                            \
    mm_err_t _err = (call);                                       \
    if (_err != MM_OK) {                                          \
      fprintf(stderr, "%s failed: %s\n", #call, mm_last_error()); \
      exit(1);                                                    \
    }                                                             \
  } while (0)

static void sleep_next(
    mm_poller_t poller,
    size_t* index,
    mm_msg_part_t* parts,
    size_t parts_cap,
    size_t* n) {
  for (;;) {
    mm_err_t err = mm_poller_next(poller, index, parts, parts_cap, n);
    if (err == MM_OK) {
      return;
    }
    if (err != MM_ENOMSG) {
      fprintf(stderr, "mm_poller_next failed: %s\n", mm_last_error());
      exit(1);
    }
    usleep(1000);
  }
}

static void poll_next(
    mm_poller_t poller,
    int fd,
    size_t* index,
    mm_msg_part_t* parts,
    size_t parts_cap,
    size_t* n) {
  for (;;) {
    mm_err_t err = mm_poller_next(poller, index, parts, parts_cap, n);
    if (err == MM_OK) {
      return;
    }
    if (err != MM_ENOMSG) {
      fprintf(stderr, "mm_poller_next failed: %s\n", mm_last_error());
      exit(1);
    }

    struct pollfd pfd = {.fd = fd, .events = POLLIN};
    if (poll(&pfd, 1, /*timeout_ms=*/-1) < 0) {
      perror("poll");
      exit(1);
    }
  }
}

int main(void) {
  mm_ctx_t ctx = NULL;
  CHECK(mm_ctx_create(&ctx));

  const char* ident_bytes = "hello-actor";
  mm_msg_part_t ident = {
      .data = ident_bytes,
      .len = strlen(ident_bytes),
      .deleter = NULL,
      .deleter_ctx = NULL,
  };
  mm_actor_t actor = NULL;
  CHECK(mm_actor_create(ctx, &ident, &actor));

  /* Send a message before subscribing to show that the actor buffers it. */
  const char* payload = "hello, self";
  mm_msg_part_t part = {
      .data = payload,
      .len = strlen(payload),
      .deleter = NULL,
      .deleter_ctx = NULL,
  };
  mm_msg_t msg = {.parts = &part, .n_parts = 1};
  mm_msg_part_t receiver = {
      .data = ident_bytes,
      .len = strlen(ident_bytes),
      .deleter = NULL,
      .deleter_ctx = NULL,
  };
  CHECK(mm_actor_send(actor, receiver, &msg));
  printf("sent message to self\n");
  fflush(stdout);

  /* Set up a poller so we can wait for incoming messages. */
  mm_poller_t poller = NULL;
  int fd = -1;
  CHECK(mm_poller_create(ctx, &fd, &poller));
  CHECK(mm_poller_subscribe(poller, 0, actor));

  /* The send is asynchronous, so the message may already be buffered or may
   * arrive after the poller is subscribed. */
  size_t index = 0;
  mm_msg_part_t parts[1];
  size_t n = 0;
  sleep_next(poller, &index, parts, 1, &n);
  printf("received: %.*s\n", (int)parts[0].len, (const char*)parts[0].data);
  fflush(stdout);

  /* Arm the wakeup fd by draining to MM_ENOMSG, then wait for a later send. */
  mm_err_t empty = mm_poller_next(poller, &index, parts, 1, &n);
  if (empty != MM_ENOMSG) {
    fprintf(stderr, "expected MM_ENOMSG from drained poller\n");
    exit(1);
  }

  CHECK(mm_actor_send(actor, receiver, &msg));
  printf("sent message to self again\n");
  fflush(stdout);

  poll_next(poller, fd, &index, parts, 1, &n);
  printf("received: %.*s\n", (int)parts[0].len, (const char*)parts[0].data);

  const char* child_ident_bytes = "child-actor";
  mm_msg_part_t child_ident = {
      .data = child_ident_bytes,
      .len = strlen(child_ident_bytes),
      .deleter = NULL,
      .deleter_ctx = NULL,
  };
  mm_actor_t child = NULL;
  CHECK(mm_actor_create(ctx, &child_ident, &child));

  mm_poller_t child_poller = NULL;
  int child_fd = -1;
  CHECK(mm_poller_create(ctx, &child_fd, &child_poller));
  CHECK(mm_poller_subscribe(child_poller, 0, child));

  const char* url = "inproc://hello-child";
  mm_connect_args_t parent_args = {
      .role = MM_PARENT,
      .name_for_other = NULL,
      .hello_prefix = NULL,
      .failure_prefix = NULL,
  };
  mm_connect_args_t child_args = {
      .role = MM_CHILD,
      .name_for_other = NULL,
      .hello_prefix = NULL,
      .failure_prefix = NULL,
  };
  CHECK(mm_actor_serve(actor, url, &parent_args));
  CHECK(mm_actor_join(child, url, &child_args));

  const char* parent_to_child = "hello, child";
  mm_msg_part_t parent_to_child_part = {
      .data = parent_to_child,
      .len = strlen(parent_to_child),
      .deleter = NULL,
      .deleter_ctx = NULL,
  };
  mm_msg_t parent_to_child_msg = {.parts = &parent_to_child_part, .n_parts = 1};
  mm_msg_part_t child_receiver = {
      .data = child_ident_bytes,
      .len = strlen(child_ident_bytes),
      .deleter = NULL,
      .deleter_ctx = NULL,
  };
  CHECK(mm_actor_send(actor, child_receiver, &parent_to_child_msg));
  poll_next(child_poller, child_fd, &index, parts, 1, &n);
  printf(
      "child received: %.*s\n", (int)parts[0].len, (const char*)parts[0].data);

  const char* child_to_parent = "hello, parent";
  mm_msg_part_t child_to_parent_part = {
      .data = child_to_parent,
      .len = strlen(child_to_parent),
      .deleter = NULL,
      .deleter_ctx = NULL,
  };
  mm_msg_t child_to_parent_msg = {.parts = &child_to_parent_part, .n_parts = 1};
  mm_msg_part_t parent_receiver = {
      .data = ident_bytes,
      .len = strlen(ident_bytes),
      .deleter = NULL,
      .deleter_ctx = NULL,
  };
  CHECK(mm_actor_send(child, parent_receiver, &child_to_parent_msg));
  poll_next(poller, fd, &index, parts, 1, &n);
  printf(
      "parent received: %.*s\n", (int)parts[0].len, (const char*)parts[0].data);

  const char* child2_ident_bytes = "child2-actor";
  mm_msg_part_t child2_ident = {
      .data = child2_ident_bytes,
      .len = strlen(child2_ident_bytes),
      .deleter = NULL,
      .deleter_ctx = NULL,
  };
  mm_actor_t child2 = NULL;
  CHECK(mm_actor_create(ctx, &child2_ident, &child2));

  const char* grandchild2_ident_bytes = "grandchild2-actor";
  mm_msg_part_t grandchild2_ident = {
      .data = grandchild2_ident_bytes,
      .len = strlen(grandchild2_ident_bytes),
      .deleter = NULL,
      .deleter_ctx = NULL,
  };
  mm_actor_t grandchild2 = NULL;
  CHECK(mm_actor_create(ctx, &grandchild2_ident, &grandchild2));

  const char* child2_url = "inproc://hello-child2";
  CHECK(mm_actor_serve(actor, child2_url, &parent_args));
  CHECK(mm_actor_join(child2, child2_url, &child_args));

  const char* grandchild2_url = "inproc://hello-grandchild2";
  CHECK(mm_actor_serve(child2, grandchild2_url, &parent_args));
  CHECK(mm_actor_join(grandchild2, grandchild2_url, &child_args));

  const char* grandchild2_to_child = "hello from grandchild2";
  mm_msg_part_t grandchild2_to_child_part = {
      .data = grandchild2_to_child,
      .len = strlen(grandchild2_to_child),
      .deleter = NULL,
      .deleter_ctx = NULL,
  };
  mm_msg_t grandchild2_to_child_msg = {
      .parts = &grandchild2_to_child_part,
      .n_parts = 1,
  };
  CHECK(mm_actor_send(grandchild2, child_receiver, &grandchild2_to_child_msg));
  poll_next(child_poller, child_fd, &index, parts, 1, &n);
  printf(
      "child received from grandchild2: %.*s\n",
      (int)parts[0].len,
      (const char*)parts[0].data);

  mm_actor_destroy(grandchild2);
  mm_actor_destroy(child2);
  mm_poller_unsubscribe(child_poller, 0);
  mm_poller_destroy(child_poller);
  mm_actor_destroy(child);
  mm_poller_unsubscribe(poller, 0);
  mm_poller_destroy(poller);
  mm_actor_destroy(actor);
  mm_ctx_destroy(ctx);
  return 0;
}
