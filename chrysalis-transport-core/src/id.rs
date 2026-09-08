/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

/// Identifies the driver that exclusively owns a connection.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct DriverId(u16);

impl DriverId {
    /// Constructs a driver ID from its process-local value.
    pub const fn from_u16(value: u16) -> Self {
        Self(value)
    }

    /// Returns the process-local value.
    pub const fn as_u16(self) -> u16 {
        self.0
    }
}

/// Identifies one QUIC connection and its exclusive owner.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ConnectionId {
    driver: DriverId,
    connection: u64,
}

impl ConnectionId {
    /// Constructs a connection ID from its owning driver and driver-local value.
    pub const fn new(driver: DriverId, connection: u64) -> Self {
        Self { driver, connection }
    }

    /// Returns the driver that exclusively owns this connection.
    pub const fn driver(self) -> DriverId {
        self.driver
    }

    /// Returns the driver-local value.
    pub const fn as_u64(self) -> u64 {
        self.connection
    }
}

/// Identifies one bidirectional stream and the driver that must process its operations.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct StreamId {
    connection: ConnectionId,
    stream: u64,
}

impl StreamId {
    /// Constructs a stream ID from its connection and QUIC stream number.
    pub const fn new(connection: ConnectionId, stream: u64) -> Self {
        Self { connection, stream }
    }

    /// Returns the driver that exclusively owns this stream's connection.
    pub const fn driver(self) -> DriverId {
        self.connection.driver()
    }

    /// Returns the connection that contains this stream.
    pub const fn connection(self) -> ConnectionId {
        self.connection
    }

    /// Returns the QUIC stream number.
    pub const fn stream(self) -> u64 {
        self.stream
    }
}

/// Correlates one accepted submission with its eventual completion.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct OperationId {
    driver: DriverId,
    sequence: u64,
}

/// Correlates one accepted driver command with its eventual completion.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RequestId {
    driver: DriverId,
    sequence: u64,
}

impl RequestId {
    pub(crate) const fn new(driver: DriverId, sequence: u64) -> Self {
        Self { driver, sequence }
    }

    /// Returns the driver that accepted the command.
    pub const fn driver(self) -> DriverId {
        self.driver
    }

    /// Returns the command sequence number within its driver.
    pub const fn sequence(self) -> u64 {
        self.sequence
    }
}

impl OperationId {
    pub(crate) const fn new(driver: DriverId, sequence: u64) -> Self {
        Self { driver, sequence }
    }

    /// Returns the driver that accepted the operation.
    pub const fn driver(self) -> DriverId {
        self.driver
    }

    /// Returns the operation sequence number within its driver.
    pub const fn sequence(self) -> u64 {
        self.sequence
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stream_id_retains_driver_connection_and_stream() {
        let connection = ConnectionId::new(DriverId::from_u16(3), 5);
        let stream = StreamId::new(connection, 7);

        assert_eq!(stream.driver().as_u16(), 3);
        assert_eq!(stream.connection().as_u64(), 5);
        assert_eq!(stream.stream(), 7);
    }

    #[test]
    fn connection_id_retains_its_driver() {
        let connection = ConnectionId::new(DriverId::from_u16(3), 5);

        assert_eq!(connection.driver(), DriverId::from_u16(3));
        assert_eq!(connection.as_u64(), 5);
    }

    #[test]
    fn operation_id_is_scoped_to_its_driver() {
        let operation = OperationId::new(DriverId::from_u16(3), 11);

        assert_eq!(operation.driver(), DriverId::from_u16(3));
        assert_eq!(operation.sequence(), 11);
    }

    #[test]
    fn request_id_is_scoped_to_its_driver() {
        let request = RequestId::new(DriverId::from_u16(3), 13);

        assert_eq!(request.driver(), DriverId::from_u16(3));
        assert_eq!(request.sequence(), 13);
    }
}
