use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;
use futures::channel::mpsc;
use nativelink_util::action_messages::OperationId;
use nativelink_error::Error;
use tokio::fs::File;
use tokio::io::{AsyncRead, AsyncReadExt, ReadBuf};
use tokio::sync::watch;
use tokio::time::{Instant, Sleep};
use pin_project::pin_project;
use crate::awaited_action_db::AwaitedActionSubscriber;

use crate::awaited_action_db::AwaitedAction;

pub struct SubscriptionHandler {

}

pub struct Subscriber {
    /// The receiver to listen for changes.
    awaited_action_rx: watch::Receiver<AwaitedAction>,
    /// Drop tx
    drop_tx: mpsc::UnboundedSender<OperationId>
}

impl AwaitedActionSubscriber for Subscriber {
    async fn changed(&mut self) -> Result<AwaitedAction, Error> {
        self.awaited_action_rx.changed().await;
        Ok(self.awaited_action_rx.borrow().clone())
    }

    fn borrow(&self) -> AwaitedAction {
        self.awaited_action_rx.borrow().clone()
    }
}

#[pin_project]
struct SlowRead<R> {
    #[pin]
    reader: R,
    #[pin]
    sleep: Sleep,
}

impl<R> AsyncRead for SlowRead<R>
where
    R: AsyncRead + Unpin,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let mut this = self.project();

        match this.sleep.as_mut().poll(cx) {
            Poll::Ready(_) => {
                this.sleep.reset(Instant::now() + Duration::from_millis(25));
                this.reader.poll_read(cx, buf)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}
