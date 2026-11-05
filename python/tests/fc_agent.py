#!/usr/bin/python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# PID 1 inside a Firecracker guest started by the Firecracker test launcher.
#
# Mounts the platform and program disks over their host paths, connects to the
# launcher's control port over vsock, runs the command it is given unchanged
# (failing instead if its working directory does not exist in the guest),
# forwards signals to the command's process group, reports the command's exit,
# then reboots so Firecracker exits.

import ctypes
import json
import os
import signal
import socket
import subprocess
import sys
import threading
import time

HOST_CID = 2
CONTROL_PORT = 9000


def log(msg: str) -> None:
    print(f"fc_agent: {msg}", file=sys.stderr, flush=True)


def sh(*args: str) -> None:
    subprocess.run(args, check=False)


def mount_all() -> None:
    sh("mount", "-t", "proc", "proc", "/proc")
    sh("mount", "-t", "sysfs", "sys", "/sys")
    sh("mount", "-t", "devtmpfs", "dev", "/dev")
    sh("mount", "-t", "tmpfs", "tmpfs", "/run")
    sh("mount", "-t", "tmpfs", "tmpfs", "/tmp")
    sh("mount", "-o", "ro", "/dev/vdb", "/platform")
    sh("mount", "-o", "ro", "/dev/vdc", "/program")
    sh("mount", "--bind", "/platform/usr/local/fbcode", "/usr/local/fbcode")
    for path in ("/data", "/home"):
        if os.path.isdir("/program" + path):
            sh("mount", "--bind", "/program" + path, path)
    sh("ip", "link", "set", "lo", "up")


def main() -> None:
    os.environ["PATH"] = "/usr/sbin:/usr/bin:/sbin:/bin"
    mount_all()
    control = socket.socket(socket.AF_VSOCK, socket.SOCK_STREAM)
    for _ in range(200):
        try:
            control.connect((HOST_CID, CONTROL_PORT))
            break
        except OSError:
            time.sleep(0.05)
    reader = control.makefile("r")
    command = json.loads(reader.readline())
    send_lock = threading.Lock()

    def send(msg: dict[str, object]) -> None:
        with send_lock:
            control.sendall((json.dumps(msg) + "\n").encode())

    cwd = command["cwd"]
    if not os.path.isdir(cwd):
        send(
            {
                "event": "exit",
                "exit_code": None,
                "signal": None,
                "failed_reason": f"working directory {cwd} does not exist in the guest",
            }
        )
        control.close()
        return
    child = subprocess.Popen(
        command["argv"],
        executable=command["executable"],
        env=command["env"],
        cwd=cwd,
        start_new_session=True,
    )
    send({"event": "started", "kernel": os.uname().release})

    def control_loop() -> None:
        for line in reader:
            msg = json.loads(line)
            if "signal" in msg and child.poll() is None:
                try:
                    os.killpg(child.pid, msg["signal"])
                except ProcessLookupError:
                    pass

    threading.Thread(target=control_loop, daemon=True).start()

    rc = child.wait()
    exit_code, sig = (rc, None) if rc >= 0 else (None, -rc)
    send({"event": "exit", "exit_code": exit_code, "signal": sig})
    control.close()


if __name__ == "__main__":
    try:
        main()
    except Exception:  # noqa: B902
        import traceback

        log(f"failed: {traceback.format_exc()}")
    finally:
        os.sync()
        libc = ctypes.CDLL(None, use_errno=True)
        libc.reboot(0x01234567)  # LINUX_REBOOT_CMD_RESTART; Firecracker exits
        signal.pause()
