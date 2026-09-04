// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

use std::io;

use thiserror::Error;
use tokio::io::AsyncRead;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;

use crate::CodecError;
use crate::Message;
use crate::codec::LENGTH_PREFIX_LEN;
use crate::decode_frame;
use crate::encode_frame;
use crate::frame_body_len;

/// An I/O or framing failure on a nameserver message stream.
#[derive(Debug, Error)]
pub enum MessageStreamError {
    /// The underlying byte stream failed.
    #[error("nameserver stream I/O failed: {0}")]
    Io(#[from] io::Error),

    /// The stream ended after part of a frame arrived.
    #[error("nameserver stream ended inside a frame")]
    TruncatedFrame,

    /// A complete frame violated the nameserver wire protocol.
    #[error(transparent)]
    Codec(#[from] CodecError),
}

#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "the dependent link-supervisor revision consumes the framing helpers"
    )
)]
pub(crate) async fn receive_message<R>(
    reader: &mut R,
) -> Result<Option<Message>, MessageStreamError>
where
    R: AsyncRead + Unpin,
{
    let mut prefix = [0; LENGTH_PREFIX_LEN];
    let read = reader.read(&mut prefix[..1]).await?;
    if read == 0 {
        return Ok(None);
    }
    read_frame_remainder(reader, &mut prefix[1..]).await?;
    let body_len = frame_body_len(prefix)?;
    let mut frame = Vec::with_capacity(LENGTH_PREFIX_LEN + body_len);
    frame.extend_from_slice(&prefix);
    frame.resize(LENGTH_PREFIX_LEN + body_len, 0);
    read_frame_remainder(reader, &mut frame[LENGTH_PREFIX_LEN..]).await?;
    Ok(Some(decode_frame(&frame)?))
}

#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "the dependent link-supervisor revision consumes the framing helpers"
    )
)]
pub(crate) async fn send_message<W>(
    writer: &mut W,
    message: &Message,
) -> Result<(), MessageStreamError>
where
    W: AsyncWrite + Unpin,
{
    let frame = encode_frame(message)?;
    writer.write_all(&frame).await?;
    writer.flush().await?;
    Ok(())
}

#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "the dependent link-supervisor revision consumes the framing helpers"
    )
)]
async fn read_frame_remainder<R>(reader: &mut R, bytes: &mut [u8]) -> Result<(), MessageStreamError>
where
    R: AsyncRead + Unpin,
{
    match reader.read_exact(bytes).await {
        Ok(_) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => {
            Err(MessageStreamError::TruncatedFrame)
        }
        Err(error) => Err(error.into()),
    }
}

#[cfg(test)]
mod tests {
    use chrysalis_core::Pid;
    use tokio::io::AsyncWriteExt;

    use super::*;
    use crate::ProtocolVersion;
    use crate::VersionRange;

    const CHILD: Pid = Pid::from_bytes([1; 16]);

    fn hello() -> Message {
        Message::Hello {
            versions: VersionRange::try_new(ProtocolVersion::new(1), ProtocolVersion::new(2))
                .unwrap(),
            child: CHILD,
        }
    }

    #[tokio::test]
    async fn messages_round_trip_over_fragmentable_byte_stream() {
        let (mut sender, mut receiver) = tokio::io::duplex(8);
        let message = hello();
        let expected = message.clone();
        let send = tokio::spawn(async move { send_message(&mut sender, &message).await });
        assert_eq!(
            receive_message(&mut receiver).await.unwrap(),
            Some(expected)
        );
        send.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn clean_end_of_stream_is_not_a_truncated_frame() {
        let (sender, mut receiver) = tokio::io::duplex(8);
        drop(sender);
        assert_eq!(receive_message(&mut receiver).await.unwrap(), None);
    }

    #[tokio::test]
    async fn partial_prefix_is_rejected() {
        let (mut sender, mut receiver) = tokio::io::duplex(8);
        sender.write_all(&[0, 0]).await.unwrap();
        sender.shutdown().await.unwrap();
        assert!(matches!(
            receive_message(&mut receiver).await,
            Err(MessageStreamError::TruncatedFrame)
        ));
    }

    #[tokio::test]
    async fn partial_body_is_rejected() {
        let frame = encode_frame(&hello()).unwrap();
        let (mut sender, mut receiver) = tokio::io::duplex(frame.len());
        sender.write_all(&frame[..frame.len() - 1]).await.unwrap();
        sender.shutdown().await.unwrap();
        assert!(matches!(
            receive_message(&mut receiver).await,
            Err(MessageStreamError::TruncatedFrame)
        ));
    }

    #[tokio::test]
    async fn oversized_body_is_rejected_from_prefix() {
        let oversized = u32::try_from(crate::MAX_FRAME_BODY_LEN + 1)
            .unwrap()
            .to_be_bytes();
        let (mut sender, mut receiver) = tokio::io::duplex(8);
        sender.write_all(&oversized).await.unwrap();
        assert!(matches!(
            receive_message(&mut receiver).await,
            Err(MessageStreamError::Codec(CodecError::FrameTooLarge { .. }))
        ));
    }
}
