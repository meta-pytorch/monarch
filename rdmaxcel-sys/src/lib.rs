/*
 * Portions Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

// sections of code adapted from https://github.com/jonhoo/rust-ibverbs
// Copyright (c) 2016 Jon Gjengset under MIT License (MIT)

mod inner {
    #![allow(non_upper_case_globals)]
    #![allow(non_camel_case_types)]
    #![allow(non_snake_case)]
    #![allow(unused_attributes)]
    #[cfg(not(cargo))]
    use crate::ibv_wc_opcode;
    #[cfg(not(cargo))]
    use crate::ibv_wc_status;
    #[cfg(cargo)]
    include!(concat!(env!("OUT_DIR"), "/bindings.rs"));

    // Manually define ibv_wc_flags as a bitfield (bindgen isn't generating it)
    #[repr(transparent)]
    #[derive(Debug, Copy, Clone, PartialEq, Eq, Default)]
    pub struct ibv_wc_flags(pub u32);

    impl ibv_wc_flags {
        pub const IBV_WC_GRH: Self = Self(1 << 0);
        pub const IBV_WC_WITH_IMM: Self = Self(1 << 1);
        pub const IBV_WC_IP_CSUM_OK: Self = Self(1 << 2);
        pub const IBV_WC_WITH_INV: Self = Self(1 << 3);
        pub const IBV_WC_TM_SYNC_REQ: Self = Self(1 << 4);
        pub const IBV_WC_TM_MATCH: Self = Self(1 << 5);
        pub const IBV_WC_TM_DATA_VALID: Self = Self(1 << 6);
    }

    impl std::ops::BitAnd for ibv_wc_flags {
        type Output = Self;
        fn bitand(self, rhs: Self) -> Self {
            Self(self.0 & rhs.0)
        }
    }

    // Manually define ibv_access_flags as a bitfield (bindgen isn't generating it)
    #[repr(transparent)]
    #[derive(Debug, Copy, Clone, PartialEq, Eq, Default)]
    pub struct ibv_access_flags(pub u32);

    impl ibv_access_flags {
        pub const IBV_ACCESS_LOCAL_WRITE: Self = Self(1 << 0);
        pub const IBV_ACCESS_REMOTE_WRITE: Self = Self(1 << 1);
        pub const IBV_ACCESS_REMOTE_READ: Self = Self(1 << 2);
        pub const IBV_ACCESS_REMOTE_ATOMIC: Self = Self(1 << 3);
        pub const IBV_ACCESS_MW_BIND: Self = Self(1 << 4);
        pub const IBV_ACCESS_ZERO_BASED: Self = Self(1 << 5);
        pub const IBV_ACCESS_ON_DEMAND: Self = Self(1 << 6);
        pub const IBV_ACCESS_HUGETLB: Self = Self(1 << 7);
        pub const IBV_ACCESS_RELAXED_ORDERING: Self = Self(1 << 8);
        pub const IBV_ACCESS_FLUSH_GLOBAL: Self = Self(1 << 9);
        pub const IBV_ACCESS_FLUSH_PERSISTENT: Self = Self(1 << 10);
    }

    impl std::ops::BitOr for ibv_access_flags {
        type Output = Self;
        fn bitor(self, rhs: Self) -> Self {
            Self(self.0 | rhs.0)
        }
    }

    #[repr(C, packed(1))]
    #[derive(Debug, Default, Clone, Copy)]
    pub struct mlx5_wqe_ctrl_seg {
        pub opmod_idx_opcode: u32,
        pub qpn_ds: u32,
        pub signature: u8,
        pub dci_stream_channel_id: u16,
        pub fm_ce_se: u8,
        pub imm: u32,
    }

    #[repr(C)]
    #[derive(Debug, Copy, Clone)]
    pub struct ibv_wc {
        wr_id: u64,
        status: ibv_wc_status::Type,
        opcode: ibv_wc_opcode::Type,
        vendor_err: u32,
        byte_len: u32,

        /// Immediate data OR the local RKey that was invalidated depending on `wc_flags`.
        /// See `man ibv_poll_cq` for details.
        pub imm_data: u32,
        /// Local QP number of completed WR.
        ///
        /// Relevant for Receive Work Completions that are associated with an SRQ.
        pub qp_num: u32,
        /// Source QP number (remote QP number) of completed WR.
        ///
        /// Relevant for Receive Work Completions of a UD QP.
        pub src_qp: u32,
        /// Flags of the Work Completion. It is either 0 or the bitwise OR of one or more of the
        /// following flags:
        ///
        ///  - `IBV_WC_GRH`: Indicator that GRH is present for a Receive Work Completions of a UD QP.
        ///    If this bit is set, the first 40 bytes of the buffered that were referred to in the
        ///    Receive request will contain the GRH of the incoming message. If this bit is cleared,
        ///    the content of those first 40 bytes is undefined
        ///  - `IBV_WC_WITH_IMM`: Indicator that imm_data is valid. Relevant for Receive Work
        ///    Completions
        pub wc_flags: ibv_wc_flags,
        /// P_Key index (valid only for GSI QPs).
        pub pkey_index: u16,
        /// Source LID (the base LID that this message was sent from).
        ///
        /// Relevant for Receive Work Completions of a UD QP.
        pub slid: u16,
        /// Service Level (the SL LID that this message was sent with).
        ///
        /// Relevant for Receive Work Completions of a UD QP.
        pub sl: u8,
        /// Destination LID path bits.
        ///
        /// Relevant for Receive Work Completions of a UD QP (not applicable for multicast messages).
        pub dlid_path_bits: u8,
    }

    #[allow(clippy::len_without_is_empty)]
    impl ibv_wc {
        /// Returns the 64 bit value that was associated with the corresponding Work Request.
        pub fn wr_id(&self) -> u64 {
            self.wr_id
        }

        /// Returns the number of bytes transferred.
        ///
        /// Relevant if the Receive Queue for incoming Send or RDMA Write with immediate operations.
        /// This value doesn't include the length of the immediate data, if such exists. Relevant in
        /// the Send Queue for RDMA Read and Atomic operations.
        ///
        /// For the Receive Queue of a UD QP that is not associated with an SRQ or for an SRQ that is
        /// associated with a UD QP this value equals to the payload of the message plus the 40 bytes
        /// reserved for the GRH. The number of bytes transferred is the payload of the message plus
        /// the 40 bytes reserved for the GRH, whether or not the GRH is present
        pub fn len(&self) -> usize {
            self.byte_len as usize
        }

        /// Check if this work requested completed successfully.
        ///
        /// A successful work completion (`IBV_WC_SUCCESS`) means that the corresponding Work Request
        /// (and all of the unsignaled Work Requests that were posted previous to it) ended, and the
        /// memory buffers that this Work Request refers to are ready to be (re)used.
        pub fn is_valid(&self) -> bool {
            self.status == ibv_wc_status::IBV_WC_SUCCESS
        }

        /// Returns the work completion status and vendor error syndrome (`vendor_err`) if the work
        /// request did not completed successfully.
        ///
        /// Possible statuses include:
        ///
        ///  - `IBV_WC_LOC_LEN_ERR`: Local Length Error: this happens if a Work Request that was posted
        ///    in a local Send Queue contains a message that is greater than the maximum message size
        ///    that is supported by the RDMA device port that should send the message or an Atomic
        ///    operation which its size is different than 8 bytes was sent. This also may happen if a
        ///    Work Request that was posted in a local Receive Queue isn't big enough for holding the
        ///    incoming message or if the incoming message size if greater the maximum message size
        ///    supported by the RDMA device port that received the message.
        ///  - `IBV_WC_LOC_QP_OP_ERR`: Local QP Operation Error: an internal QP consistency error was
        ///    detected while processing this Work Request: this happens if a Work Request that was
        ///    posted in a local Send Queue of a UD QP contains an Address Handle that is associated
        ///    with a Protection Domain to a QP which is associated with a different Protection Domain
        ///    or an opcode which isn't supported by the transport type of the QP isn't supported (for
        ///    example:
        ///    RDMA Write over a UD QP).
        ///  - `IBV_WC_LOC_EEC_OP_ERR`: Local EE Context Operation Error: an internal EE Context
        ///    consistency error was detected while processing this Work Request (unused, since its
        ///    relevant only to RD QPs or EE Context, which aren’t supported).
        ///  - `IBV_WC_LOC_PROT_ERR`: Local Protection Error: the locally posted Work Request’s buffers
        ///    in the scatter/gather list does not reference a Memory Region that is valid for the
        ///    requested operation.
        ///  - `IBV_WC_WR_FLUSH_ERR`: Work Request Flushed Error: A Work Request was in process or
        ///    outstanding when the QP transitioned into the Error State.
        ///  - `IBV_WC_MW_BIND_ERR`: Memory Window Binding Error: A failure happened when tried to bind
        ///    a MW to a MR.
        ///  - `IBV_WC_BAD_RESP_ERR`: Bad Response Error: an unexpected transport layer opcode was
        ///    returned by the responder. Relevant for RC QPs.
        ///  - `IBV_WC_LOC_ACCESS_ERR`: Local Access Error: a protection error occurred on a local data
        ///    buffer during the processing of a RDMA Write with Immediate operation sent from the
        ///    remote node. Relevant for RC QPs.
        ///  - `IBV_WC_REM_INV_REQ_ERR`: Remote Invalid Request Error: The responder detected an
        ///    invalid message on the channel. Possible causes include the operation is not supported
        ///    by this receive queue (qp_access_flags in remote QP wasn't configured to support this
        ///    operation), insufficient buffering to receive a new RDMA or Atomic Operation request, or
        ///    the length specified in a RDMA request is greater than 2^{31} bytes. Relevant for RC
        ///    QPs.
        ///  - `IBV_WC_REM_ACCESS_ERR`: Remote Access Error: a protection error occurred on a remote
        ///    data buffer to be read by an RDMA Read, written by an RDMA Write or accessed by an
        ///    atomic operation. This error is reported only on RDMA operations or atomic operations.
        ///    Relevant for RC QPs.
        ///  - `IBV_WC_REM_OP_ERR`: Remote Operation Error: the operation could not be completed
        ///    successfully by the responder. Possible causes include a responder QP related error that
        ///    prevented the responder from completing the request or a malformed WQE on the Receive
        ///    Queue. Relevant for RC QPs.
        ///  - `IBV_WC_RETRY_EXC_ERR`: Transport Retry Counter Exceeded: The local transport timeout
        ///    retry counter was exceeded while trying to send this message. This means that the remote
        ///    side didn't send any Ack or Nack. If this happens when sending the first message,
        ///    usually this mean that the connection attributes are wrong or the remote side isn't in a
        ///    state that it can respond to messages. If this happens after sending the first message,
        ///    usually it means that the remote QP isn't available anymore. Relevant for RC QPs.
        ///  - `IBV_WC_RNR_RETRY_EXC_ERR`: RNR Retry Counter Exceeded: The RNR NAK retry count was
        ///    exceeded. This usually means that the remote side didn't post any WR to its Receive
        ///    Queue. Relevant for RC QPs.
        ///  - `IBV_WC_LOC_RDD_VIOL_ERR`: Local RDD Violation Error: The RDD associated with the QP
        ///    does not match the RDD associated with the EE Context (unused, since its relevant only
        ///    to RD QPs or EE Context, which aren't supported).
        ///  - `IBV_WC_REM_INV_RD_REQ_ERR`: Remote Invalid RD Request: The responder detected an
        ///    invalid incoming RD message. Causes include a Q_Key or RDD violation (unused, since its
        ///    relevant only to RD QPs or EE Context, which aren't supported)
        ///  - `IBV_WC_REM_ABORT_ERR`: Remote Aborted Error: For UD or UC QPs associated with a SRQ,
        ///    the responder aborted the operation.
        ///  - `IBV_WC_INV_EECN_ERR`: Invalid EE Context Number: An invalid EE Context number was
        ///    detected (unused, since its relevant only to RD QPs or EE Context, which aren't
        ///    supported).
        ///  - `IBV_WC_INV_EEC_STATE_ERR`: Invalid EE Context State Error: Operation is not legal for
        ///    the specified EE Context state (unused, since its relevant only to RD QPs or EE Context,
        ///    which aren't supported).
        ///  - `IBV_WC_FATAL_ERR`: Fatal Error.
        ///  - `IBV_WC_RESP_TIMEOUT_ERR`: Response Timeout Error.
        ///  - `IBV_WC_GENERAL_ERR`: General Error: other error which isn't one of the above errors.
        pub fn error(&self) -> Option<(ibv_wc_status::Type, u32)> {
            match self.status {
                ibv_wc_status::IBV_WC_SUCCESS => None,
                status => Some((status, self.vendor_err)),
            }
        }

        /// Returns the operation that the corresponding Work Request performed.
        ///
        /// This value controls the way that data was sent, the direction of the data flow and the
        /// valid attributes in the Work Completion.
        pub fn opcode(&self) -> ibv_wc_opcode::Type {
            self.opcode
        }

        /// Returns a 32 bits number, in network order, in an SEND or RDMA WRITE opcodes that is being
        /// sent along with the payload to the remote side and placed in a Receive Work Completion and
        /// not in a remote memory buffer
        ///
        /// Note that IMM is only returned if `IBV_WC_WITH_IMM` is set in `wc_flags`. If this is not
        /// the case, no immediate value was provided, and `imm_data` should be interpreted
        /// differently. See `man ibv_poll_cq` for details.
        pub fn imm_data(&self) -> Option<u32> {
            if self.is_valid() && ((self.wc_flags & ibv_wc_flags::IBV_WC_WITH_IMM).0 != 0) {
                Some(self.imm_data)
            } else {
                None
            }
        }
    }

    impl Default for ibv_wc {
        fn default() -> Self {
            ibv_wc {
                wr_id: 0,
                status: ibv_wc_status::IBV_WC_GENERAL_ERR,
                opcode: ibv_wc_opcode::IBV_WC_LOCAL_INV,
                vendor_err: 0,
                byte_len: 0,
                imm_data: 0,
                qp_num: 0,
                src_qp: 0,
                wc_flags: ibv_wc_flags(0),
                pkey_index: 0,
                slid: 0,
                sl: 0,
                dlid_path_bits: 0,
            }
        }
    }
}

pub use inner::*;

// Segment scanner callback type - type alias for the bindgen-generated type
pub type RdmaxcelSegmentScannerFn = rdmaxcel_segment_scanner_fn;

// Additional extern "C" declarations for functions that are also auto-generated by bindgen.
// These provide a place for doc comments and explicit signatures.
unsafe extern "C" {
    pub fn rdmaxcel_error_string(error_code: std::os::raw::c_int) -> *const std::os::raw::c_char;
    pub fn get_cuda_pci_address_from_ptr(
        cuda_ptr: u64,
        pci_addr_out: *mut std::os::raw::c_char,
        pci_addr_size: usize,
    ) -> std::os::raw::c_int;

    /// Debug: Print comprehensive device attributes
    pub fn rdmaxcel_print_device_info(context: *mut ibv_context);
}
