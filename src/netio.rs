//! Socket settings and a write-idle timeout for accepted connections.
//!
//! A response body holds its share of the in-flight budget until hyper has
//! written it. If the peer vanishes or stops reading mid-response, the write
//! would otherwise wait for the kernel to give up on the connection (about
//! 15 minutes with default tcp_retries2), pinning up to 64 MiB of budget.

use std::{
    future::Future,
    io,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use socket2::{SockRef, TcpKeepalive};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::TcpStream,
    time::{Instant, Sleep},
};

/// Keepalive probes after `timeout` idle, and on Linux TCP_USER_TIMEOUT so
/// unacknowledged data fails the connection after `timeout` too.
pub fn tune_socket(stream: &TcpStream, timeout: Duration) {
    let _ = stream.set_nodelay(true);
    let sock = SockRef::from(stream);
    let ka = TcpKeepalive::new()
        .with_time(timeout)
        .with_interval(Duration::from_secs(10));
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    let ka = ka.with_retries(3);
    let _ = sock.set_tcp_keepalive(&ka);
    #[cfg(target_os = "linux")]
    let _ = sock.set_tcp_user_timeout(Some(timeout));
}

/// Fails a write that makes no progress for `timeout`. Progress on any
/// write resets the clock, so slow but live readers are fine.
pub struct WriteIdleTimeout<S> {
    inner: S,
    timeout: Duration,
    sleep: Option<Pin<Box<Sleep>>>,
}

impl<S> WriteIdleTimeout<S> {
    pub fn new(inner: S, timeout: Duration) -> Self {
        Self {
            inner,
            timeout,
            sleep: None,
        }
    }

    /// Called when a write returned Pending: arm the timer, or report a
    /// timeout if it already ran out.
    fn pending(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let timeout = self.timeout;
        let sleep = self
            .sleep
            .get_or_insert_with(|| Box::pin(tokio::time::sleep_until(Instant::now() + timeout)));
        match sleep.as_mut().poll(cx) {
            Poll::Ready(()) => Poll::Ready(Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "peer stopped reading",
            ))),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for WriteIdleTimeout<S> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for WriteIdleTimeout<S> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match Pin::new(&mut self.inner).poll_write(cx, buf) {
            Poll::Ready(r) => {
                self.sleep = None;
                Poll::Ready(r)
            }
            Poll::Pending => self.pending(cx).map(|r| r.map(|()| 0)),
        }
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        match Pin::new(&mut self.inner).poll_write_vectored(cx, bufs) {
            Poll::Ready(r) => {
                self.sleep = None;
                Poll::Ready(r)
            }
            Poll::Pending => self.pending(cx).map(|r| r.map(|()| 0)),
        }
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match Pin::new(&mut self.inner).poll_flush(cx) {
            Poll::Ready(r) => {
                self.sleep = None;
                Poll::Ready(r)
            }
            Poll::Pending => self.pending(cx),
        }
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match Pin::new(&mut self.inner).poll_shutdown(cx) {
            Poll::Ready(r) => {
                self.sleep = None;
                Poll::Ready(r)
            }
            Poll::Pending => self.pending(cx),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncWriteExt;

    #[tokio::test(start_paused = true)]
    async fn stalled_write_times_out() {
        // A 1 KiB pipe whose reader never reads.
        let (a, _b) = tokio::io::duplex(1024);
        let mut w = WriteIdleTimeout::new(a, Duration::from_secs(30));
        let big = vec![0u8; 64 * 1024];
        let err = w.write_all(&big).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::TimedOut);
    }

    #[tokio::test(start_paused = true)]
    async fn slow_reader_keeps_the_connection() {
        let (a, mut b) = tokio::io::duplex(1024);
        let mut w = WriteIdleTimeout::new(a, Duration::from_secs(30));
        let reader = tokio::spawn(async move {
            let mut buf = [0u8; 512];
            let mut n = 0;
            while n < 16 * 1024 {
                tokio::time::sleep(Duration::from_secs(10)).await;
                n += tokio::io::AsyncReadExt::read(&mut b, &mut buf)
                    .await
                    .unwrap();
            }
        });
        w.write_all(&vec![1u8; 16 * 1024]).await.unwrap();
        reader.await.unwrap();
    }
}
