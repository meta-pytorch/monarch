import os
import subprocess
import sys
import time
from typing import Callable, TypeVar

import cloudpickle
from monarch.actor import this_host


T = TypeVar("T")

from monarch.actor import Actor, endpoint


class Lambda(Actor):
    @endpoint
    def run(self, l: Callable[[], T]) -> T:
        return l()


class Nest(Actor):
    def __init__(self):
        self.nest = this_host().spawn_procs().spawn("nested", Lambda)

    @endpoint
    def nested(self, l: Callable[[], T]) -> T:
        return self.nest.run.broadcast(l)

    @endpoint
    def nested_call_one(self, l: Callable[[], T]) -> T:
        return self.nest.run.call_one(l).get()

    @endpoint
    def direct(self, l: Callable[[], T]) -> T:
        return l()

    @endpoint
    def kill_nest(self) -> None:
        pid = self.nest.run.call_one(lambda: os.getpid()).get()
        os.kill(pid, 9)


def error():
    print("I AM ABOUT TO ERROR!!!!")
    raise ValueError("Error.")


class SuperviseNest(Nest):
    def __supervise__(self, x):
        print("SUPERVISE: ", x)


def actor_failure_proc():
    l = this_host().spawn_procs().spawn("actor", Lambda)
    l.run.broadcast(error)
    time.sleep(10)


def run_subprocess(l: Callable[[], None]):
    # serialize l via cloudpickle.
    pickled = cloudpickle.dumps(l)
    # run sys.executable -c 'cloudpickle.loads({repr of cloudpickle bytes})()'
    code = f"import cloudpickle; cloudpickle.loads({pickled!r})()"
    result = subprocess.run([sys.executable, "-c", code])
    # return the exit code
    return result.returncode


def test_actor_failure():
    """
    If an actor dies, the client should die.

    If we have qualms about killing the client, we should provide a monarch.panic_hook that normall
    kills the process, but can be configured to do something else.

        monarch.panic_hook = ...
    """
    assert 0 != run_subprocess(actor_failure_proc)


def run_subprocess_proc_failure():
    l = this_host().spawn_procs().spawn("actor", Nest)
    print("Killed ", l.kill_nest.call_one().get())
    time.sleep(10)


def test_proc_failure():
    """
    If a proc dies, the client should die.
    """

    assert 0 != run_subprocess(run_subprocess_proc_failure)


def nested_mesh_kills_actor():
    l = this_host().spawn_procs().spawn("actor", Nest)
    v = l.nested_call_one.call_one(lambda: 4).get()
    print(v)
    assert v == 4
    l.kill_nest.call_one().get()
    print("KILLED!")
    for i in range(10):
        l.direct.call_one(lambda: None).get()
        print("Nest still alive", i)
        time.sleep(1)
    raise RuntimeError("Nest was never killed?")


def nested_mesh_kills_actor_actor_error():
    l = this_host().spawn_procs().spawn("actor", Nest)
    v = l.nested_call_one.call_one(lambda: 4).get()
    l.nested.call_one(error).get()
    print("ERRORED THE ACTOR")
    for i in range(10):
        l.direct.call_one(lambda: None).get()
        print("Nest still alive", i)
        time.sleep(1)
    print("Nest was never killed")


def test_nested_mesh_kills_actor_actor_error():
    assert 0 != run_subprocess(nested_mesh_kills_actor_actor_error)


def a_dead_man_tells_no_tales():
    l = this_host().spawn_procs().spawn("actor", Lambda)
    l.run.broadcast(error)
    assert 4 == l.run.call_one(lambda: 4).get()
    print("HOW DID I GET 4? The actor should have died before it got this message.")


def test_a_dead_man_tells_no_tales():
    assert 0 != run_subprocess(a_dead_man_tells_no_tales)
