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
    target: &U::ProtocolAddress,
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
    type ProtocolAddress: Clone;
    fn local_addr(&self) -> std::io::Result<Self::ProtocolAddress>;
    fn peer_addr(&self) -> std::io::Result<Self::ProtocolAddress>;
    fn recv_buf(&self, buf: &mut impl BufMut) -> impl Future<Output = std::io::Result<usize>>;
    fn recv_buf_from(
        &self,
        buf: &mut impl BufMut,
    ) -> impl Future<Output = std::io::Result<(usize, Self::ProtocolAddress)>>;
    fn send(&self, buf: &[u8]) -> impl Future<Output = std::io::Result<usize>>;
    fn send_to(
        &self,
        buf: &[u8],
        target: &Self::ProtocolAddress,
    ) -> impl Future<Output = std::io::Result<usize>>;
    fn send_vectored(&self, bufs: &[IoSlice<'_>]) -> impl Future<Output = std::io::Result<usize>>;
    fn send_to_vectored(
        &self,
        bufs: &[IoSlice<'_>],
        target: &Self::ProtocolAddress,
    ) -> impl Future<Output = std::io::Result<usize>>;
    fn try_send(&self, buf: &[u8]) -> std::io::Result<usize>;
    fn try_send_to(&self, buf: &[u8], target: &Self::ProtocolAddress) -> std::io::Result<usize>;
    fn is_send_vectored(&self) -> bool;
}
impl UnreliableTransmit for tokio::net::UdpSocket {
    type ProtocolAddress = SocketAddr;
    fn local_addr(&self) -> std::io::Result<SocketAddr> {
        self.local_addr()
    }
    fn peer_addr(&self) -> std::io::Result<SocketAddr> {
        self.peer_addr()
    }
    async fn recv_buf(&self, buf: &mut impl BufMut) -> std::io::Result<usize> {
        self.recv_buf(buf).await
    }
    async fn recv_buf_from(
        &self,
        buf: &mut impl BufMut,
    ) -> std::io::Result<(usize, Self::ProtocolAddress)> {
        self.recv_buf_from(buf).await
    }
    async fn send(&self, buf: &[u8]) -> std::io::Result<usize> {
        self.send(buf).await
    }
    async fn send_to(&self, buf: &[u8], target: &Self::ProtocolAddress) -> std::io::Result<usize> {
        self.send_to(buf, target).await
    }
    fn try_send(&self, buf: &[u8]) -> std::io::Result<usize> {
        self.try_send(buf)
    }
    fn try_send_to(&self, buf: &[u8], target: &Self::ProtocolAddress) -> std::io::Result<usize> {
        self.try_send_to(buf, *target)
    }
    async fn send_vectored(&self, bufs: &[IoSlice<'_>]) -> std::io::Result<usize> {
        default_send_vectored(self, bufs).await
    }
    async fn send_to_vectored(
        &self,
        bufs: &[IoSlice<'_>],
        target: &Self::ProtocolAddress,
    ) -> std::io::Result<usize> {
        default_send_to_vectored(self, bufs, target).await
    }
    fn is_send_vectored(&self) -> bool {
        false
    }
}

impl UnreliableTransmit for tokio_udp::UdpSocket {
    type ProtocolAddress = SocketAddr;
    fn local_addr(&self) -> std::io::Result<SocketAddr> {
        self.local_addr()
    }
    fn peer_addr(&self) -> std::io::Result<SocketAddr> {
        self.peer_addr()
    }
    async fn recv_buf(&self, buf: &mut impl BufMut) -> std::io::Result<usize> {
        self.recv_buf(buf).await
    }
    async fn recv_buf_from(
        &self,
        buf: &mut impl BufMut,
    ) -> std::io::Result<(usize, Self::ProtocolAddress)> {
        self.recv_buf_from(buf).await
    }
    async fn send(&self, buf: &[u8]) -> std::io::Result<usize> {
        self.send(buf).await
    }
    async fn send_to(&self, buf: &[u8], target: &Self::ProtocolAddress) -> std::io::Result<usize> {
        self.send_to_vectored(&[IoSlice::new(buf)], target).await
    }
    fn try_send(&self, buf: &[u8]) -> std::io::Result<usize> {
        self.try_send(buf)
    }
    fn try_send_to(&self, buf: &[u8], target: &Self::ProtocolAddress) -> std::io::Result<usize> {
        self.try_send_to(buf, target)
    }
    async fn send_vectored(&self, bufs: &[IoSlice<'_>]) -> std::io::Result<usize> {
        self.send_vectored(bufs).await
    }
    async fn send_to_vectored(
        &self,
        bufs: &[IoSlice<'_>],
        target: &Self::ProtocolAddress,
    ) -> std::io::Result<usize> {
        self.send_to_vectored(bufs, target).await
    }
    fn is_send_vectored(&self) -> bool {
        tokio_udp::is_vectored_supported()
    }
}
