# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

import pickle

from monarch._rust_bindings.monarch_hyperactor.channel import ChannelTransport


def test_pickle_channel_transport() -> None:
    for transport in (
        ChannelTransport.TcpWithLocalhost,
        ChannelTransport.TcpWithHostname,
        ChannelTransport.MetaTlsWithHostname,
        ChannelTransport.MetaTlsWithIpV6,
        ChannelTransport.Tls,
        ChannelTransport.Local,
        ChannelTransport.Unix,
    ):
        pickled = pickle.dumps(transport)
        unpickled = pickle.loads(pickled)
        assert unpickled == transport
