#!/usr/bin/env bash
#
# Build an rdma-core source tree whose ionic provider matches AMD AINIC kernel
# drivers that speak the vendor ionic uverbs ABI (4), e.g. ionic_rdma 25.08.x.
#
# Upstream rdma-core's ionic provider only accepts ABI 1 (the in-tree Linux
# driver). On AINIC 25.08 hosts it rejects every device ("does not support the
# kernel ABI of 4"), and even with the check relaxed its CQ polling fails.
# AMD publishes the matching provider source as an rdma-core src.rpm; this
# script overlays that provider onto the rdma-core commit monarch pins and
# applies the small ports needed for the newer libibverbs.
#
# The output directory is meant for MONARCH_RDMA_CORE_SRC:
#
#   ./make_rdma_core_ainic.sh /opt/rdma-core-ainic
#   MONARCH_RDMA_CORE_SRC=/opt/rdma-core-ainic <build monarch>
#
# Knobs (environment):
#   AINIC_CHANNEL            repo.radeon.com AINIC channel  (default 1.117.1-a-63)
#   AINIC_OS                 channel OS directory           (default el8)
#   AINIC_RDMA_CORE_VERSION  AMD rdma-core src.rpm version  (default 54.0.149.g3304be71)
#   AINIC_SRC_RPM            use this local src.rpm instead of downloading
#   AINIC_SRC_RPM_SHA256     expected sha256; defaults to the pinned value for the
#                            default version, empty for any other version (no check)
#   RDMA_CORE_REPO           rdma-core git URL (default upstream GitHub)
#   RDMA_CORE_TAG            rdma-core commit (default: read from ../build.rs)
#
# Pick the AINIC channel matching the host's AINIC install (firmware/driver);
# the provider must match the kernel driver's ABI.
set -euo pipefail

usage() { sed -n '2,/^set -euo/p' "$0" | sed 's/^# \{0,1\}//;$d'; exit "${1:-0}"; }
[[ $# -eq 1 && $1 != -h && $1 != --help ]] || usage $(( $# == 1 ? 0 : 2 ))

OUT_DIR=$1
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

DEFAULT_VERSION=54.0.149.g3304be71
DEFAULT_SHA256=b996ff0c410baf2d670a5ce5c4297799378670bbd7b0fe0271c800fbb8dc45b5
AINIC_CHANNEL=${AINIC_CHANNEL:-1.117.1-a-63}
AINIC_OS=${AINIC_OS:-el8}
AINIC_RDMA_CORE_VERSION=${AINIC_RDMA_CORE_VERSION:-$DEFAULT_VERSION}
if [[ -z ${AINIC_SRC_RPM_SHA256+x} ]]; then
  if [[ $AINIC_RDMA_CORE_VERSION == "$DEFAULT_VERSION" ]]; then
    AINIC_SRC_RPM_SHA256=$DEFAULT_SHA256
  else
    AINIC_SRC_RPM_SHA256=
  fi
fi
RDMA_CORE_REPO=${RDMA_CORE_REPO:-https://github.com/linux-rdma/rdma-core}
if [[ -z ${RDMA_CORE_TAG:-} ]]; then
  RDMA_CORE_TAG=$(sed -n 's/^const RDMA_CORE_TAG: &str = "\([0-9a-f]*\)";/\1/p' "$HERE/../build.rs")
  [[ -n $RDMA_CORE_TAG ]] || { echo "ERROR: could not read RDMA_CORE_TAG from $HERE/../build.rs" >&2; exit 1; }
fi

RPM_NAME=rdma-core-${AINIC_RDMA_CORE_VERSION}-1.${AINIC_OS}.src.rpm
RPM_URL=https://repo.radeon.com/amdainic/pensando/${AINIC_OS}/${AINIC_CHANNEL}/${RPM_NAME}

if [[ -e $OUT_DIR ]]; then
  echo "ERROR: $OUT_DIR already exists; remove it or pick another path" >&2
  exit 1
fi

WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT

echo "==> rdma-core ${RDMA_CORE_TAG} from ${RDMA_CORE_REPO}"
git clone --quiet --filter=blob:none --no-checkout "$RDMA_CORE_REPO" "$OUT_DIR"
git -C "$OUT_DIR" -c advice.detachedHead=false checkout --quiet "$RDMA_CORE_TAG"
if [[ ! -d $OUT_DIR/providers/ionic ]]; then
  echo "ERROR: rdma-core ${RDMA_CORE_TAG} has no providers/ionic; it predates the ionic provider" >&2
  exit 1
fi

if [[ -n ${AINIC_SRC_RPM:-} ]]; then
  echo "==> AMD src.rpm (local): $AINIC_SRC_RPM"
  cp "$AINIC_SRC_RPM" "$WORK/$RPM_NAME"
else
  echo "==> AMD src.rpm: $RPM_URL"
  curl -fsSL -o "$WORK/$RPM_NAME" "$RPM_URL"
fi
if [[ -n $AINIC_SRC_RPM_SHA256 ]]; then
  echo "$AINIC_SRC_RPM_SHA256  $WORK/$RPM_NAME" | sha256sum -c --quiet -
else
  echo "WARNING: no AINIC_SRC_RPM_SHA256 for ${AINIC_RDMA_CORE_VERSION}; not verifying the download" >&2
fi

# A src.rpm is a cpio archive holding the source tarball + spec. Use whichever
# unpacker the machine has.
mkdir "$WORK/rpm"
if command -v rpm2archive >/dev/null; then
  rpm2archive - < "$WORK/$RPM_NAME" | tar -xz -C "$WORK/rpm"
elif command -v bsdtar >/dev/null; then
  bsdtar -xf "$WORK/$RPM_NAME" -C "$WORK/rpm"
elif command -v rpm2cpio >/dev/null && command -v cpio >/dev/null; then
  (cd "$WORK/rpm" && rpm2cpio "$WORK/$RPM_NAME" | cpio -idm --quiet)
else
  echo "ERROR: need one of rpm2archive, bsdtar, or rpm2cpio+cpio to unpack $RPM_NAME" >&2
  exit 1
fi
TARBALL=$WORK/rpm/rdma-core-${AINIC_RDMA_CORE_VERSION}.tar.gz
[[ -f $TARBALL ]] || { echo "ERROR: $RPM_NAME does not contain $(basename "$TARBALL")" >&2; exit 1; }
tar -xzf "$TARBALL" -C "$WORK"
AMD_SRC=$WORK/rdma-core-${AINIC_RDMA_CORE_VERSION}

echo "==> overlay AMD ionic provider + ionic uverbs ABI header"
rm -rf "$OUT_DIR/providers/ionic"
cp -a "$AMD_SRC/providers/ionic" "$OUT_DIR/providers/ionic"
cp "$AMD_SRC/kernel-headers/rdma/ionic-abi.h" "$OUT_DIR/kernel-headers/rdma/ionic-abi.h"

for p in "$HERE"/patches/*.patch; do
  echo "==> patch $(basename "$p")"
  patch -d "$OUT_DIR" -p1 --forward --no-backup-if-mismatch --quiet < "$p"
done

ABI=$(sed -n 's/^#define IONIC_ABI_VERSION[[:space:]]*\([0-9]*\).*/\1/p' "$OUT_DIR/kernel-headers/rdma/ionic-abi.h")

cat > "$OUT_DIR/AINIC_PROVENANCE" <<PROV
rdma_core_repo=${RDMA_CORE_REPO}
rdma_core_commit=${RDMA_CORE_TAG}
ainic_src_rpm=${RPM_URL}
ainic_src_rpm_sha256=$(sha256sum "$WORK/$RPM_NAME" | cut -d' ' -f1)
ainic_rdma_core_version=${AINIC_RDMA_CORE_VERSION}
ionic_abi_version=${ABI}
patches=$(cd "$HERE/patches" && ls *.patch | paste -sd, -)
PROV

echo "==> done: $OUT_DIR (ionic uverbs ABI ${ABI})"
echo "    MONARCH_RDMA_CORE_SRC=$OUT_DIR"
