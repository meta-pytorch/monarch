# ionic provider for AMD AINIC hosts

AMD AINIC installs (e.g. `ionic_rdma` 25.08.x, firmware 1.117.x) ship an out-of-tree
ionic RDMA driver that uses its own uverbs ABI (`/sys/class/infiniband_verbs/uverbs*/abi_version`
reads `4` for `ionic_*` devices). The ionic provider in upstream rdma-core only accepts ABI 1,
which is what the in-tree Linux driver (6.18+) uses. On these hosts it refuses the devices:

    libibverbs: Warning: Driver ionic does not support the kernel ABI of 4 (supports 1 to 1)

`make_rdma_core_ainic.sh` builds an rdma-core source tree at monarch's pinned commit with the
ionic provider replaced by AMD's matching one. AMD publishes that provider as an rdma-core src.rpm
on repo.radeon.com. The script also applies the ports in `patches/`. Point the monarch build at
the result with `MONARCH_RDMA_CORE_SRC`.

    ./make_rdma_core_ainic.sh /opt/rdma-core-ainic
    MONARCH_RDMA_CORE_SRC=/opt/rdma-core-ainic <build monarch>

- The AINIC channel/version must match the host's AINIC install (`AINIC_CHANNEL`,
  `AINIC_RDMA_CORE_VERSION`); see the script header.
- Only the ionic provider and `kernel-headers/rdma/ionic-abi.h` change. libibverbs and the other
  providers stay at monarch's pinned commit.
- `AINIC_PROVENANCE` in the output records the inputs.
