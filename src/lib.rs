use std::convert::Infallible;
use std::fmt;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll, ready};

use futures::stream::Fuse;
use futures::{Stream, StreamExt, TryStream, TryStreamExt};

pub trait Sink<T> {
    type Error;

    // paused? via Poll::Pending
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        item: T,
    ) -> Poll<Result<(), Self::Error>>;

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>>;

    // closed?
    fn is_terminated(&self) -> bool;
}

pub trait Source {
    type Error;

    // pending? via Poll::Pending
    fn poll_resume(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>>;

    fn poll_abort(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>>;

    // ended?
    fn is_terminated(&self) -> bool;
}

// -----

pub struct SendAll<'a, Si, St>
where
    Si: ?Sized,
    St: ?Sized + TryStream,
{
    sink: &'a mut Si,
    stream: Fuse<&'a mut St>,
    terminated: bool,
}

// Pinning is never projected to any fields.
impl<Si, St> Unpin for SendAll<'_, Si, St>
where
    Si: Unpin + ?Sized,
    St: TryStream + Unpin + ?Sized,
{
}

impl<Si, St> fmt::Debug for SendAll<'_, Si, St>
where
    Si: fmt::Debug + ?Sized,
    St: fmt::Debug + ?Sized + TryStream,
    St::Ok: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SendAll")
            .field("sink", &self.sink)
            .field("stream", &self.stream)
            .finish()
    }
}

impl<'a, Si, St, Ok, Error> SendAll<'a, Si, St>
where
    Si: Sink<Ok, Error = Error> + Unpin + ?Sized,
    St: TryStream<Ok = Ok, Error = Error> + Stream + Unpin + ?Sized,
{
    pub fn new(sink: &'a mut Si, stream: &'a mut St) -> Self {
        Self {
            sink,
            stream: stream.fuse(),
            terminated: false,
        }
    }
}

impl<'a, Si, St, Ok, Error> Source for SendAll<'_, Si, St>
where
    Si: Sink<Ok, Error = Error> + Unpin + ?Sized,
    St: Stream<Item = Result<Ok, Error>> + Unpin + ?Sized,
{
    type Error = Error;

    fn poll_resume(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();

        loop {
            match this.stream.try_poll_next_unpin(cx) {
                Poll::Ready(Some(Ok(item))) => {
                    println!("> produce item from stream");
                    if this.sink.is_terminated() {
                        return Poll::Ready(Ok(()));
                    }
                    ready!(Pin::new(&mut *this.sink).poll_write(cx, item))?;
                }
                Poll::Ready(Some(Err(err))) => {
                    this.terminated = true;
                    if !this.sink.is_terminated() {
                        ready!(Pin::new(&mut *this.sink).poll_close(cx))?;
                    }
                    return Poll::Ready(Err(err));
                }
                Poll::Ready(None) => {
                    println!("stream terminated");
                    this.terminated = true;
                    if !this.sink.is_terminated() {
                        ready!(Pin::new(&mut *this.sink).poll_close(cx))?;
                    }
                    return Poll::Ready(Ok(()));
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }

    fn poll_abort(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();
        this.terminated = true;

        let mut sink = Pin::new(&mut *this.sink);
        if !sink.is_terminated() {
            ready!(sink.as_mut().poll_close(cx))?;
        }

        Poll::Ready(Ok(()))
    }

    fn is_terminated(&self) -> bool {
        self.terminated
    }
}

impl<'a, Si, St, Ok, Error> Future for SendAll<'_, Si, St>
where
    Si: Sink<Ok, Error = Error> + Unpin + ?Sized,
    St: Stream<Item = Result<Ok, Error>> + Unpin + ?Sized,
{
    type Output = Result<(), Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.as_mut().poll_resume(cx)
    }
}

// -----

pub struct Processor<Item, Si> {
    sink: Si,
    terminated: bool,
    _marker: PhantomData<Item>,
}

impl<Item, Si> Processor<Item, Si> {
    pub fn new(sink: Si) -> Self {
        Self {
            sink,
            terminated: false,
            _marker: PhantomData,
        }
    }
}

impl<Item, Si> Source for Processor<Item, Si>
where
    Si: Sink<Item> + Unpin,
    Item: Unpin,
{
    type Error = Si::Error;

    fn poll_resume(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        println!("worky worky");
        Poll::Ready(Ok(()))
    }

    fn poll_abort(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();
        this.terminated = true;

        let mut sink = Pin::new(&mut this.sink);
        if !sink.is_terminated() {
            ready!(sink.as_mut().poll_close(cx))?;
        }

        Poll::Ready(Ok(()))
    }

    fn is_terminated(&self) -> bool {
        self.terminated
    }
}

impl<Item, Si> Sink<Item> for Processor<Item, Si>
where
    Si: Sink<Item> + Unpin,
    Item: Unpin,
{
    type Error = Si::Error;

    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        item: Item,
    ) -> Poll<Result<(), Self::Error>> {
        println!("worky worky");
        let this = self.get_mut();
        let mut sink = Pin::new(&mut this.sink);
        ready!(sink.as_mut().poll_write(cx, item))?;
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        println!("close");
        let this = self.get_mut();
        this.terminated = true;

        let mut sink = Pin::new(&mut this.sink);
        if !sink.is_terminated() {
            ready!(sink.as_mut().poll_close(cx))?;
        }

        Poll::Ready(Ok(()))
    }

    fn is_terminated(&self) -> bool {
        self.terminated
    }
}

// -----

pub struct Print<Item> {
    terminated: bool,
    _marker: PhantomData<Item>,
}

impl<Item> Print<Item> {
    pub fn new() -> Self {
        Self {
            terminated: false,
            _marker: PhantomData,
        }
    }
}

impl<Item> Sink<Item> for Print<Item>
where
    Item: fmt::Debug,
{
    type Error = Infallible;

    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        item: Item,
    ) -> Poll<Result<(), Self::Error>> {
        println!("consume {:?}", item);
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn is_terminated(&self) -> bool {
        self.terminated
    }
}

#[cfg(test)]
mod tests {
    use futures::executor::LocalPool;
    use futures::stream;
    use futures::task::LocalSpawnExt;

    use super::{Print, Processor, SendAll};

    #[test]
    fn it_works() {
        let mut pool = LocalPool::new();
        let spawner = pool.spawner();

        spawner
            .spawn_local(async {
                let consumer = Print::new();
                let transformer_3 = Processor::new(consumer);
                let transformer_2 = Processor::new(transformer_3);
                let mut transformer = Processor::new(transformer_2);

                let mut stream = stream::iter(vec![Ok(1), Ok(2), Ok(3)]);
                let producer = SendAll::new(&mut transformer, &mut stream);

                // Start executing the pipeline, this calls poll_resume.
                let result = producer.await;

                println!("{:?}", result);
            })
            .unwrap();

        pool.run();
    }
}
