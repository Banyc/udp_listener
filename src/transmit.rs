use core::{future::Future, net::SocketAddr};
use std::io::IoSlice;

use bytes::BufMut;

async fn default_send_vectored<U: UnreliableTransmit>(
    this: &U,
    bufs: &[IoSlice<'_>],
) -> std::io::Result<usize> {
    match bufs.len() {
        0 => Ok(0),
        1 => this.send(&bufs[0]).await,
        _ => {
            let total = bufs.iter().map(|b| b.len()).sum();
            let mut buf = Vec::with_capacity(total);
            for b in bufs {
                buf.extend_from_slice(b);
            }
            this.send(&buf).await
        }
    }
}

async fn default_send_to_vectored<U: UnreliableTransmit>(
    this: &U,
    bufs: &[IoSlice<'_>],
    target: &SocketAddr,
) -> std::io::Result<usize> {
    match bufs.len() {
        0 => Ok(0),
        1 => this.send_to(&bufs[0], target).await,
        _ => {
            let total = bufs.iter().map(|b| b.len()).sum();
            let mut buf = Vec::with_capacity(total);
            for b in bufs {
                buf.extend_from_slice(b);
            }
            this.send_to(&buf, target).await
        }
    }
}

pub trait UnreliableTransmit {
    fn local_addr(&self) -> std::io::Result<SocketAddr>;
    fn peer_addr(&self) -> std::io::Result<SocketAddr>;
    fn recv_buf(&self, buf: &mut impl BufMut) -> impl Future<Output = std::io::Result<usize>>;
    fn recv_buf_from(
        &self,
        buf: &mut impl BufMut,
    ) -> impl Future<Output = std::io::Result<(usize, SocketAddr)>>;
    fn send(&self, buf: &[u8]) -> impl Future<Output = std::io::Result<usize>>;
    fn send_to(
        &self,
        buf: &[u8],
        target: &SocketAddr,
    ) -> impl Future<Output = std::io::Result<usize>>;
    fn send_vectored(&self, bufs: &[IoSlice<'_>]) -> impl Future<Output = std::io::Result<usize>>;
    fn send_to_vectored(
        &self,
        bufs: &[IoSlice<'_>],
        target: &SocketAddr,
    ) -> impl Future<Output = std::io::Result<usize>>;
    fn try_send(&self, buf: &[u8]) -> std::io::Result<usize>;
    fn try_send_to(&self, buf: &[u8], target: &SocketAddr) -> std::io::Result<usize>;
    fn supports_send_vectored(&self) -> bool;
}
impl UnreliableTransmit for tokio::net::UdpSocket {
    fn local_addr(&self) -> std::io::Result<SocketAddr> {
        self.local_addr()
    }
    fn peer_addr(&self) -> std::io::Result<SocketAddr> {
        self.peer_addr()
    }
    async fn recv_buf(&self, buf: &mut impl BufMut) -> std::io::Result<usize> {
        self.recv_buf(buf).await
    }
    async fn recv_buf_from(&self, buf: &mut impl BufMut) -> std::io::Result<(usize, SocketAddr)> {
        self.recv_buf_from(buf).await
    }
    async fn send(&self, buf: &[u8]) -> std::io::Result<usize> {
        self.send(buf).await
    }
    async fn send_to(&self, buf: &[u8], target: &SocketAddr) -> std::io::Result<usize> {
        self.send_to(buf, target).await
    }
    fn try_send(&self, buf: &[u8]) -> std::io::Result<usize> {
        self.try_send(buf)
    }
    fn try_send_to(&self, buf: &[u8], target: &SocketAddr) -> std::io::Result<usize> {
        self.try_send_to(buf, *target)
    }
    async fn send_vectored(&self, bufs: &[IoSlice<'_>]) -> std::io::Result<usize> {
        default_send_vectored(self, bufs).await
    }
    async fn send_to_vectored(
        &self,
        bufs: &[IoSlice<'_>],
        target: &SocketAddr,
    ) -> std::io::Result<usize> {
        default_send_to_vectored(self, bufs, target).await
    }
    fn supports_send_vectored(&self) -> bool {
        false
    }
}

impl UnreliableTransmit for tokio_udp::UdpSocket {
    fn local_addr(&self) -> std::io::Result<SocketAddr> {
        self.local_addr()
    }
    fn peer_addr(&self) -> std::io::Result<SocketAddr> {
        self.peer_addr()
    }
    async fn recv_buf(&self, buf: &mut impl BufMut) -> std::io::Result<usize> {
        self.recv_buf(buf).await
    }
    async fn recv_buf_from(&self, buf: &mut impl BufMut) -> std::io::Result<(usize, SocketAddr)> {
        self.recv_buf_from(buf).await
    }
    async fn send(&self, buf: &[u8]) -> std::io::Result<usize> {
        self.send(buf).await
    }
    async fn send_to(&self, buf: &[u8], target: &SocketAddr) -> std::io::Result<usize> {
        self.send_to_vectored(&[IoSlice::new(buf)], target).await
    }
    fn try_send(&self, buf: &[u8]) -> std::io::Result<usize> {
        self.try_send(buf)
    }
    fn try_send_to(&self, buf: &[u8], target: &SocketAddr) -> std::io::Result<usize> {
        self.try_send_to(buf, target)
    }
    async fn send_vectored(&self, bufs: &[IoSlice<'_>]) -> std::io::Result<usize> {
        self.send_vectored(bufs).await
    }
    async fn send_to_vectored(
        &self,
        bufs: &[IoSlice<'_>],
        target: &SocketAddr,
    ) -> std::io::Result<usize> {
        self.send_to_vectored(bufs, target).await
    }
    fn supports_send_vectored(&self) -> bool {
        tokio_udp::is_vectored_supported()
    }
}

#[cfg(test)]
mod tests {
    use std::io::IoSlice;

    use crate::UnreliableTransmit;

    /// `tokio::net::UdpSocket` has no vectorized syscall, so its connected
    /// vectored send concatenates every buffer into one temporary and sends
    /// them as a single datagram; a fallback that drops all but the first
    /// buffer would silently truncate the packet.
    #[tokio::test]
    async fn tokio_net_udp_connected_vectored_send_concatenates_every_buffer() {
        let a = tokio::net::UdpSocket::bind(("127.0.0.1", 0)).await.unwrap();
        let b = tokio::net::UdpSocket::bind(("127.0.0.1", 0)).await.unwrap();
        a.connect(b.local_addr().unwrap()).await.unwrap();
        let bufs = [
            IoSlice::new(b"ab"),
            IoSlice::new(b"cd"),
            IoSlice::new(b"ef"),
        ];
        assert_eq!(
            UnreliableTransmit::send_vectored(&a, &bufs).await.unwrap(),
            6
        );
        let mut buf = [0u8; 16];
        let (n, _) = b.recv_from(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"abcdef");

        // A single buffer takes the direct path.
        assert_eq!(
            UnreliableTransmit::send_vectored(&a, &[IoSlice::new(b"x")])
                .await
                .unwrap(),
            1
        );
        let (n, _) = b.recv_from(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"x");

        // No buffers sends nothing.
        assert_eq!(UnreliableTransmit::send_vectored(&a, &[]).await.unwrap(), 0);
    }

    /// The unconnected vectored send concatenates buffers, forwards a single
    /// buffer directly, and sends nothing for an empty slice list.
    #[tokio::test]
    async fn tokio_net_udp_vectored_send_concatenates_every_buffer() {
        let a = tokio::net::UdpSocket::bind(("127.0.0.1", 0)).await.unwrap();
        let b = tokio::net::UdpSocket::bind(("127.0.0.1", 0)).await.unwrap();
        let b_addr = b.local_addr().unwrap();
        let bufs = [
            IoSlice::new(b"ab"),
            IoSlice::new(b"cd"),
            IoSlice::new(b"ef"),
        ];
        assert_eq!(
            UnreliableTransmit::send_to_vectored(&a, &bufs, &b_addr)
                .await
                .unwrap(),
            6
        );
        let mut buf = [0u8; 16];
        let (n, _) = b.recv_from(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"abcdef");

        assert_eq!(
            UnreliableTransmit::send_to_vectored(&a, &[IoSlice::new(b"x")], &b_addr)
                .await
                .unwrap(),
            1
        );
        let (n, _) = b.recv_from(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"x");

        assert_eq!(
            UnreliableTransmit::send_to_vectored(&a, &[], &b_addr)
                .await
                .unwrap(),
            0
        );
    }

    /// Whether a transport can avoid the concatenation fallback is a property
    /// of its backend: the native socket reports `is_vectored_supported()`, the
    /// `tokio::net` wrapper never does.
    #[cfg(unix)]
    #[tokio::test]
    async fn supports_send_vectored_matches_the_backend() {
        let native = tokio_udp::UdpSocket::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        assert_eq!(
            UnreliableTransmit::supports_send_vectored(&native),
            tokio_udp::is_vectored_supported()
        );
        let net = tokio::net::UdpSocket::bind(("127.0.0.1", 0)).await.unwrap();
        assert!(!UnreliableTransmit::supports_send_vectored(&net));
    }
}
