# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""
Distributed Web Crawler
==============================================================
This example demonstrates how to make a simplistic distributed web crawler with
Monarch actors including:

- Creating a singleton QueueActor
- Providing that QueueActor to multiple CrawlActors
- Having CrawlActors add/remove items from the QueueActor as they crawl
- Retrieving results and cleaning up

The queue uses a local deque and stores response ports for waiters when empty.
An auxiliary set is used to avoid duplicates. Thread-safety is not required
because in Monarch each actor handles its messages sequentially.
"""

# %%
"""
Import libraries and set tuneable configuration values.
"""
import time
from collections import deque
from typing import Optional, Set, Tuple
from urllib.parse import urlparse, urlunparse

import requests
from bs4 import BeautifulSoup
from monarch.actor import Actor, context, endpoint, Port, ProcMesh, this_host

# Configuration
BASE = "https://meta-pytorch.org/monarch/"
DEPTH = 3
NUM_CRAWLERS = 8
TIMEOUT = 5


# %%
class QueueActor(Actor):
    """
    Define the QueueActor class.
    - Holds a deque for items and a list of waiting ports.
    - When get() is called and queue is empty, the port is stored.
    - When insert() is called, it either wakes a waiter or enqueues the item.
    """

    def __init__(self):
        self.q: deque = deque()
        self.seen_links: Set[str] = set()
        self.waiters: deque[Port[Optional[Tuple[str, int]]]] = deque()

    @endpoint
    def insert(self, item: str, depth: int) -> None:
        if item not in self.seen_links:
            self.seen_links.add(item)
            if self.waiters:
                port = self.waiters.popleft()
                port.send((item, depth))
            else:
                self.q.append((item, depth))

    @endpoint(explicit_response_port=True)
    def get(self, port: Port[Optional[Tuple[str, int]]]) -> None:
        if self.q:
            port.send(self.q.popleft())
        else:
            self.waiters.append(port)


# %%
class CrawlActor(Actor):
    """
    Define the CrawlActor class.
    - Takes in all queues, but slices down to only use the first one.
    - Runs a crawl() process that continuously takes items off the central queue,
      parses them, and adds links it finds back to the queue.
    - Crawls to a configured depth and terminates after the queue is empty for
      a configured number of seconds.
    """

    def __init__(self, all_queues: QueueActor):
        self.target_queue: QueueActor = all_queues.slice(procs=slice(0, 1))
        self.processed = 0

    @staticmethod
    def normalize_url(url: str) -> str:
        p = urlparse(url)
        normalized = urlunparse((p.scheme, p.netloc, p.path, p.params, "", ""))
        return normalized

    def _crawl_internal(self, target: str, depth: int) -> None:
        response = requests.get(target)
        response_size_kb = len(response.content) / 1024
        print(f"    - {target} was {response_size_kb:.2f} KB")
        parsed = BeautifulSoup(response.content, "html.parser")

        anchors = parsed.find_all("a", href=True)
        for a in anchors:
            link = a["href"] if "https://" in a["href"] else BASE + a["href"]

            # Stop at the target depth and only follow links on our base site.
            if depth > 0 and BASE in link:
                normalized_link = CrawlActor.normalize_url(link)
                self.target_queue.insert.broadcast(normalized_link, depth - 1)

    @endpoint
    def crawl(self) -> int:
        rank = context().actor_instance.rank

        while True:
            try:
                result = self.target_queue.get.call_one().get(timeout=TIMEOUT)
            except TimeoutError:
                print("Queue has no items, returning done value.")
                result = None
            if result is None:
                break
            url, depth = result
            print(f"Crawler #{rank} found {url} @ depth={depth}.")
            self._crawl_internal(url, depth)
            self.processed += 1

        return self.processed


# %%
def main():
    start_time = time.time()

    # Start up a ProcMesh.
    local_proc_mesh: ProcMesh = this_host().spawn_procs(
        per_host={"procs": NUM_CRAWLERS}
    )

    # Create queues across the mesh and use slice to target the first one.
    all_queues = local_proc_mesh.spawn("queues", QueueActor)
    target_queue = all_queues.slice(procs=slice(0, 1))

    # Prime the queue with the base URL we want to crawl.
    target_queue.insert.broadcast(BASE, DEPTH)

    # Make the crawlers and pass in the queues.
    crawlers = local_proc_mesh.spawn("crawlers", CrawlActor, all_queues)

    # Run the crawlers; display the count of documents they crawled when done.
    results = crawlers.crawl.call().get()

    # Shut down all our resources.
    local_proc_mesh.stop().get()

    # Log results.
    pages = sum(v[1] for v in results.items())
    duration = time.time() - start_time
    print(f"Finished - Found {pages} in {duration:.2f} seconds.\n{results}.")


# %%
"""
Run main.
"""
main()

# %%
# Results
# -----------
# With NUM_CRAWLERS=1, this takes around 288 seconds:
#
# .. code-block:: text
#
#     Finished - Found 3123 in 288.07 seconds.
#
#     ValueMesh({procs: 1}):
#      (({'procs': 0/1}, 3123),).
#
# With NUM_CRAWLERS=8, this takes around 45 seconds:
#
# .. code-block:: text
#
#     Finished - Found 3123 in 45.94 seconds.
#
#     ValueMesh({procs: 8}):
#       (({'procs': 0/8}, 393),
#        ({'procs': 1/8}, 393),
#        ({'procs': 2/8}, 397),
#        ({'procs': 3/8}, 394),
#        ({'procs': 4/8}, 383),
#        ({'procs': 5/8}, 393),
#        ({'procs': 6/8}, 393),
#        ({'procs': 7/8}, 377)).
#
# So, we see a near-linear improvement in crawling time from
# the concurrent crawlers using the central queue.
