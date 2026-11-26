#!/bin/bash
# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# Goes one step past kvm-check.sh: gives a VM 64 KiB of memory holding a single
# HLT instruction at the x86 reset vector, then calls KVM_RUN once.
echo "host: $(hostname)  uid: $(id -u)  kernel: $(uname -r)  memlock: $(ulimit -l)"
cg=$(awk -F: '$1=="0"{print $3}' /proc/self/cgroup)
echo "cgroup: $cg"
for f in memory.max memory.current memory.peak; do
  [ -r "/sys/fs/cgroup$cg/$f" ] && echo "  $f: $(cat "/sys/fs/cgroup$cg/$f")"
done
[ -r "/sys/fs/cgroup$cg/memory.events" ] && echo "  memory.events: $(tr '\n' ' ' < "/sys/fs/cgroup$cg/memory.events")"
[ "$(id -u)" != 0 ] || { echo "FAIL: running as root"; exit 1; }
python3 - <<'PY'
import ctypes, fcntl, mmap, os, struct, sys
kvm = os.open("/dev/kvm", os.O_RDWR)
api = fcntl.ioctl(kvm, 0xAE00)
print("KVM_GET_API_VERSION:", api)
if api != 12:
    sys.exit("FAIL: expected API version 12")
vm = fcntl.ioctl(kvm, 0xAE01, 0)                              # KVM_CREATE_VM
print("KVM_CREATE_VM: ok")
fcntl.ioctl(vm, 0xAE47, 0x7FFFD000)                           # KVM_SET_TSS_ADDR (3 pages, below 2 GiB so it fits a C int)
print("KVM_SET_TSS_ADDR: ok")
size, gpa = 0x10000, 0xFFFF0000                               # top 64 KiB below 4 GiB
mem = mmap.mmap(-1, size, mmap.MAP_PRIVATE | mmap.MAP_ANONYMOUS,
                mmap.PROT_READ | mmap.PROT_WRITE)
mem[0xFFF0] = 0xF4                                            # HLT at reset vector F000:FFF0
addr = ctypes.addressof(ctypes.c_char.from_buffer(mem))
fcntl.ioctl(vm, 0x4020AE46, struct.pack("IIQQQ", 0, 0, gpa, size, addr))  # KVM_SET_USER_MEMORY_REGION
print("KVM_SET_USER_MEMORY_REGION: ok")
vcpu = fcntl.ioctl(vm, 0xAE41, 0)                             # KVM_CREATE_VCPU
print("KVM_CREATE_VCPU: ok")
run_size = fcntl.ioctl(kvm, 0xAE04)                           # KVM_GET_VCPU_MMAP_SIZE
run = mmap.mmap(vcpu, run_size, mmap.MAP_SHARED, mmap.PROT_READ | mmap.PROT_WRITE)
try:
    fcntl.ioctl(vcpu, 0xAE80, 0)                              # KVM_RUN
except OSError as e:
    sys.exit(f"FAIL: KVM_RUN: {e}")
reason = struct.unpack_from("I", run, 8)[0]
print("KVM_RUN: returned, exit_reason", reason)
if reason != 5:                                               # KVM_EXIT_HLT
    sys.exit("FAIL: expected exit_reason 5 (KVM_EXIT_HLT)")
print("PASS")
PY
rc=$?
for f in memory.peak memory.events; do
  [ -r "/sys/fs/cgroup$cg/$f" ] && echo "after: $f: $(tr '\n' ' ' < "/sys/fs/cgroup$cg/$f")"
done
exit $rc
