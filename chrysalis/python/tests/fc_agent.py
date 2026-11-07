#!/usr/bin/python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# PID 1 inside a sealed-postcard Firecracker guest.
#
# The guest has one read-only root drive that already contains the Rust role
# binary and its runtime. The agent mounts the kernel's virtual filesystems,
# connects to the host's control port over vsock, runs the command it is given
# unchanged (failing instead if its working directory does not exist), forwards
# signals to the command's process group, reports the command's exit, then
# reboots so Firecracker exits.

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


def mount_virtual_filesystems() -> None:
    for fstype, source, target in (
        ("proc", "proc", "/proc"),
        ("sysfs", "sys", "/sys"),
        ("devtmpfs", "dev", "/dev"),
        ("tmpfs", "tmpfs", "/run"),
        ("tmpfs", "tmpfs", "/tmp"),
    ):
        # The kernel may already have mounted devtmpfs on /dev.
        if not os.path.ismount(target):
            subprocess.run(["mount", "-t", fstype, source, target], check=False)
    subprocess.run(["ip", "link", "set", "lo", "up"], check=False)


def connect_control() -> socket.socket:
    control = socket.socket(socket.AF_VSOCK, socket.SOCK_STREAM)
    for _ in range(200):
        try:
            control.connect((HOST_CID, CONTROL_PORT))
            return control
        except OSError:
            time.sleep(0.05)
    raise ConnectionError(f"no host control listener on port {CONTROL_PORT}")


def main() -> None:
    os.environ["PATH"] = "/usr/sbin:/usr/bin:/sbin:/bin"
    mount_virtual_filesystems()
    control = connect_control()
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
