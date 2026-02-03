# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

import time

begin = time.time()
from monarch.actor import Actor, endpoint, this_host


class Noop(Actor):
    @endpoint
    def run(self) -> None:
        return None


h = this_host().spawn_procs().spawn("actor", Noop)
h.run.call_one().get()
end = time.time()

print(end - begin)
