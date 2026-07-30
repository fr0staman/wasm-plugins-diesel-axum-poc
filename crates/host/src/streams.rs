//! Bridge between tokio channels and component-model streams.
//!
//! Plugins hand the host a `stream<T>` (WS replies, SSE chunks) and receive one
//! (WS client frames). Wasmtime models both ends with poll-based traits, so this
//! module provides the two adapters the axum layer needs:
//!
//! * [`ChannelProducer`] — feeds a guest stream from an mpsc receiver.
//! * [`ChannelConsumer`] — drains a guest stream into a **bounded** sender.
//!
//! Both move items in batches. One `poll_produce` / `poll_consume` is one
//! boundary crossing regardless of how many items it carries, so batching is
//! what makes streams cheaper than the per-call API they replaced: for a
//! 100-chunk SSE response it turns 100 crossings into 4.
//!
//! The consumer is where backpressure comes from: when the socket is slower than
//! the plugin, `poll_consume` returns `Poll::Pending` and the guest's write
//! blocks, instead of the host buffering without limit as the old
//! `UnboundedSender` plumbing did.

use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::PollSender;
use wasmtime::{AsContextMut, StoreContextMut};
use wasmtime::component::{
    Destination, FutureConsumer, Source, StreamConsumer, StreamProducer, StreamResult, VecBuffer,
};

/// Maximum items moved per boundary crossing.
pub const BATCH: usize = 32;

/// Feeds a guest-read stream from an mpsc receiver, draining up to [`BATCH`]
/// items per crossing.
///
/// Ends the stream (`Dropped`) when the sender half is dropped, which is how the
/// axum layer signals that the client went away.
pub struct ChannelProducer<T> {
    rx: mpsc::Receiver<T>,
}

impl<T> ChannelProducer<T> {
    pub fn new(rx: mpsc::Receiver<T>) -> Self {
        Self { rx }
    }
}

impl<T, D> StreamProducer<D> for ChannelProducer<T>
where
    T: Send + Sync + Unpin + 'static,
{
    type Item = T;
    type Buffer = VecBuffer<T>;

    fn poll_produce<'a>(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        _store: StoreContextMut<'a, D>,
        mut dst: Destination<'a, Self::Item, Self::Buffer>,
        finish: bool,
    ) -> Poll<wasmtime::Result<StreamResult>> {
        let me = self.get_mut();
        let mut buf = Vec::with_capacity(BATCH);
        match me.rx.poll_recv_many(cx, &mut buf, BATCH) {
            Poll::Ready(0) => {
                // Sender dropped: client disconnected, end of stream.
                Poll::Ready(Ok(StreamResult::Dropped))
            }
            Poll::Ready(_) => {
                dst.set_buffer(buf.into());
                Poll::Ready(Ok(StreamResult::Completed))
            }
            // Nothing available. When `finish` is set the guest is cancelling a
            // pending read, so report "no items read" rather than blocking it.
            Poll::Pending if finish => Poll::Ready(Ok(StreamResult::Cancelled)),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Delivers a guest-written `future<T>` into a oneshot channel.
///
/// Host `FutureReader`s cannot be awaited directly — like streams they are
/// consumed by piping — so this is how the terminal result of a connection gets
/// back to the caller.
pub struct OneshotConsumer<T> {
    tx: Option<oneshot::Sender<T>>,
}

impl<T> OneshotConsumer<T> {
    pub fn new(tx: oneshot::Sender<T>) -> Self {
        Self { tx: Some(tx) }
    }
}

impl<T, D> FutureConsumer<D> for OneshotConsumer<T>
where
    T: wasmtime::component::Lift + Send + Sync + Unpin + 'static,
{
    type Item = T;

    fn poll_consume(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        mut store: StoreContextMut<D>,
        mut source: Source<'_, Self::Item>,
        finish: bool,
    ) -> Poll<wasmtime::Result<()>> {
        let me = self.get_mut();
        if finish {
            // Cancelled before a value was produced; leave the oneshot dropped
            // so the receiver observes the cancellation.
            return Poll::Ready(Ok(()));
        }

        let mut buf = Vec::with_capacity(1);
        source.read(store.as_context_mut(), &mut buf)?;
        if let (Some(value), Some(tx)) = (buf.into_iter().next(), me.tx.take()) {
            let _ = tx.send(value);
        }
        Poll::Ready(Ok(()))
    }
}

/// Drains a guest-written stream into a bounded sender, taking up to [`BATCH`]
/// items per crossing.
///
/// The channel carries batches rather than items so a single reserved permit
/// covers the whole crossing. Capacity is therefore counted in batches, which is
/// what the receiving side must keep in mind when sizing it.
///
/// `Poll::Pending` on a full channel is what propagates backpressure back into
/// the plugin.
pub struct ChannelConsumer<T> {
    tx: PollSender<Vec<T>>,
}

impl<T: Send + 'static> ChannelConsumer<T> {
    pub fn new(tx: mpsc::Sender<Vec<T>>) -> Self {
        Self {
            tx: PollSender::new(tx),
        }
    }
}

impl<T, D> StreamConsumer<D> for ChannelConsumer<T>
where
    T: wasmtime::component::Lift + Send + Sync + Unpin + 'static,
{
    type Item = T;

    fn poll_consume<'a>(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        mut store: StoreContextMut<'a, D>,
        mut source: Source<'a, Self::Item>,
        finish: bool,
    ) -> Poll<wasmtime::Result<StreamResult>> {
        let me = self.get_mut();

        // Reserve capacity first: if the receiver is behind, this parks the
        // waker and the guest's write stays outstanding.
        match me.tx.poll_reserve(cx) {
            Poll::Ready(Ok(())) => {}
            // Receiver dropped: client went away, tell the guest to stop.
            Poll::Ready(Err(_)) => return Poll::Ready(Ok(StreamResult::Dropped)),
            Poll::Pending if finish => return Poll::Ready(Ok(StreamResult::Cancelled)),
            Poll::Pending => return Poll::Pending,
        }

        let mut buf = Vec::with_capacity(BATCH);
        source.read(store.as_context_mut(), &mut buf)?;

        if buf.is_empty() {
            return Poll::Ready(Ok(StreamResult::Completed));
        }
        if me.tx.send_item(buf).is_err() {
            return Poll::Ready(Ok(StreamResult::Dropped));
        }
        Poll::Ready(Ok(StreamResult::Completed))
    }
}
