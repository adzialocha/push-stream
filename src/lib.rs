use std::error::Error;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::{Sink, Stream, ready};
use pin_project_lite::pin_project;
use thiserror::Error;

pub trait Processor<T> {
    type Output;

    type Error: Error;

    type Stream: Stream<Item = Result<Self::Output, Self::Error>>;

    type Future: Future<Output = Result<Self::Stream, Self::Error>>;

    fn process(&mut self, input: T) -> Self::Future;
}

pin_project! {
    #[project = StateProjected]
    enum State<Fut, St, Out> {
        Idle,
        Process { #[pin] process_in_fut: Fut },
        Streaming { #[pin] process_out_stream: St, output: Option<Out> },
    }
}

pin_project! {
    #[must_use = "sinks do nothing unless polled"]
    pub struct ProcessorStage<P, Si, T>
    where
        P: Processor<T>,
    {
        processor: P,
        #[pin]
        inner: Si,
        #[pin]
        state: State<P::Future, P::Stream, P::Output>,
    }
}

impl<P, Si, T> ProcessorStage<P, Si, T>
where
    P: Processor<T>,
{
    pub fn new(processor: P, inner: Si) -> Self {
        Self {
            processor,
            inner,
            state: State::Idle,
        }
    }

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), P::Error>>
    where
        Si: Sink<P::Output, Error = P::Error>,
    {
        let result = self.as_mut().poll_inner(cx);
        if let Poll::Ready(Err(_)) = &result {
            self.project().state.set(State::Idle);
        }
        result
    }

    fn poll_inner(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), P::Error>>
    where
        Si: Sink<P::Output, Error = P::Error>,
    {
        let mut this = self.project();

        loop {
            match this.state.as_mut().project() {
                StateProjected::Idle => return Poll::Ready(Ok(())),
                StateProjected::Process { process_in_fut } => {
                    let process_out_stream = ready!(process_in_fut.poll(cx))?;
                    this.state.set(State::Streaming {
                        process_out_stream,
                        output: None,
                    });
                }
                StateProjected::Streaming {
                    process_out_stream,
                    output,
                } => {
                    if output.is_none() {
                        match ready!(process_out_stream.poll_next(cx)) {
                            Some(next) => *output = Some(next?),
                            None => {
                                this.state.set(State::Idle);
                                continue;
                            }
                        }
                    }

                    ready!(this.inner.as_mut().poll_ready(cx))?;
                    this.inner
                        .as_mut()
                        .start_send(output.take().expect("output"))?;
                }
            }
        }
    }
}

impl<P, Si, T> Sink<T> for ProcessorStage<P, Si, T>
where
    P: Processor<T>,
    Si: Sink<P::Output, Error = P::Error>,
{
    type Error = P::Error;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        ready!(self.as_mut().poll(cx))?;
        self.project().inner.poll_ready(cx)
    }

    fn start_send(self: Pin<&mut Self>, item: T) -> Result<(), Self::Error> {
        let mut this = self.project();
        debug_assert!(
            matches!(this.state.as_mut().project(), StateProjected::Idle),
            "start_send without idle state",
        );
        let process_in_fut = this.processor.process(item);
        this.state.set(State::Process { process_in_fut });
        Ok(())
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        ready!(self.as_mut().poll(cx))?;
        self.project().inner.poll_flush(cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        ready!(self.as_mut().poll(cx))?;
        self.project().inner.poll_close(cx)
    }
}

pub trait ProcessorExt<Out>: Sink<Out> + Sized {
    fn with_processor<P, T>(self, processor: P) -> ProcessorStage<P, Self, T>
    where
        P: Processor<T, Output = Out, Error = Self::Error>,
    {
        ProcessorStage::new(processor, self)
    }
}

impl<Si: Sink<Out>, Out> ProcessorExt<Out> for Si {}

#[derive(Debug, PartialEq, Eq, Error)]
pub enum Fused<E> {
    #[error(transparent)]
    Sink(E),

    #[error("sink is closed")]
    Closed,
}

pin_project! {
    #[must_use = "sinks do nothing unless polled"]
    pub struct Fuse<Si> {
        #[pin]
        sink: Si,
        closed: bool,
    }
}

impl<Si> Fuse<Si> {
    pub fn new(sink: Si) -> Self {
        Self {
            sink,
            closed: false,
        }
    }

    pub fn is_closed(&self) -> bool {
        self.closed
    }
}

impl<Si: Sink<Item>, Item> Sink<Item> for Fuse<Si> {
    type Error = Fused<Si::Error>;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.project();
        if *this.closed {
            return Poll::Ready(Err(Fused::Closed));
        }
        this.sink.poll_ready(cx).map_err(Fused::Sink)
    }

    fn start_send(self: Pin<&mut Self>, item: Item) -> Result<(), Self::Error> {
        let this = self.project();
        if *this.closed {
            return Err(Fused::Closed);
        }
        this.sink.start_send(item).map_err(Fused::Sink)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.project();
        if *this.closed {
            return Poll::Ready(Ok(()));
        }
        this.sink.poll_flush(cx).map_err(Fused::Sink)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.project();
        if *this.closed {
            return Poll::Ready(Ok(()));
        }
        ready!(this.sink.poll_close(cx)).map_err(Fused::Sink)?;
        *this.closed = true;
        Poll::Ready(Ok(()))
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::pin::{Pin, pin};
    use std::rc::Rc;
    use std::time::Duration;

    use futures::channel::mpsc;
    use futures::executor::block_on;
    use futures::stream::{self, StreamExt};
    use futures::{Sink, SinkExt, sink};
    use thiserror::Error;

    use super::{Fuse, Fused, Processor, ProcessorExt};

    #[derive(Debug, PartialEq, Eq, Error)]
    #[error("processor error on input={0}")]
    struct TestError(usize);

    type Collector = Rc<RefCell<Vec<usize>>>;

    fn collect(seen: Collector) -> impl Sink<usize, Error = TestError> {
        sink::unfold((), move |(), v: usize| {
            let seen = seen.clone();
            async move {
                seen.borrow_mut().push(v);
                Ok::<_, TestError>(())
            }
        })
    }

    struct Repeat {
        /// Throw error when item with this value arrives.
        fail_on: Option<usize>,
    }

    impl Processor<usize> for Repeat {
        type Output = usize;

        type Error = TestError;

        type Stream = stream::Iter<std::vec::IntoIter<Result<usize, TestError>>>;

        type Future = futures::future::Ready<Result<Self::Stream, TestError>>;

        fn process(&mut self, input: usize) -> Self::Future {
            futures::future::ready(if self.fail_on == Some(input) {
                Err(TestError(input))
            } else {
                // Repeat input as often as it's own value.
                Ok(stream::iter(
                    (0..input).map(move |_| Ok(input)).collect::<Vec<_>>(),
                ))
            })
        }
    }

    #[test]
    fn map_filter_collect() {
        let seen = Collector::default();

        block_on(async {
            let pipe = collect(seen.clone())
                .with(|x: usize| async move { Ok::<_, TestError>(x * 10) })
                .with_flat_map(|x: usize| {
                    stream::iter(if x % 2 == 0 { vec![Ok(x)] } else { vec![] })
                });
            let mut pipe = pin!(Fuse::new(pipe));

            for i in 1..=5 {
                pipe.send(i).await.unwrap();
            }

            pipe.close().await.unwrap();
        });

        assert_eq!(*seen.borrow(), vec![20, 40]);
    }

    #[test]
    fn send_resolves_only_when_drained() {
        let seen = Collector::default();

        block_on(async {
            let mut pipe = pin!(collect(seen.clone()).with_processor(Repeat { fail_on: None }));

            for ev in [3, 1, 2] {
                pipe.send(ev).await.unwrap();
                assert_eq!(
                    seen.borrow().iter().filter(|&&v| v == ev).count(),
                    ev as usize
                );
            }

            pipe.close().await.unwrap();
        });

        assert_eq!(*seen.borrow(), vec![3, 3, 3, 1, 2, 2]);
    }

    #[test]
    fn processor_error_stops_everything() {
        let seen = Collector::default();

        block_on(async {
            let mut pipe = pin!(Fuse::new(
                collect(seen.clone()).with_processor(Repeat { fail_on: Some(2) })
            ));

            pipe.send(1).await.unwrap();
            assert_eq!(pipe.send(2).await, Err(Fused::Sink(TestError(2))));
        });

        assert_eq!(*seen.borrow(), vec![1]);
    }

    #[test]
    fn close_failed_pipeline() {
        let seen = Collector::default();

        block_on(async {
            let mut pipe = pin!(collect(seen.clone()).with_processor(Repeat { fail_on: Some(2) }));

            assert_eq!(pipe.send(2).await, Err(TestError(2)));

            pipe.close().await.unwrap();
        });

        assert!(seen.borrow().is_empty());
    }

    #[test]
    fn idempotent_closing() {
        block_on(async {
            let mut pipe = pin!(Fuse::new(sink::drain::<usize>()));
            pipe.send(1).await.unwrap();
            pipe.close().await.unwrap();

            // Close should be idempotent.
            pipe.close().await.unwrap();

            // Should throw error when trying to send into closed pipeline.
            assert_eq!(pipe.send(2).await, Err(Fused::Closed));
            assert!(pipe.is_closed());
        });
    }

    #[test]
    fn graceful_close_finishes_work_first() {
        let seen = Collector::default();

        block_on(async {
            let mut pipe = pin!(collect(seen.clone()).with_processor(Repeat { fail_on: None }));

            // Send work into pipeline (without flushing) and close it directly after.
            pipe.feed(3).await.unwrap();
            pipe.close().await.unwrap();
        });

        assert_eq!(*seen.borrow(), vec![3, 3, 3]);
    }

    #[test]
    fn back_pressure() {
        block_on(async {
            let (tx, mut rx) = mpsc::channel::<usize>(0);
            let tx = tx.sink_map_err(|_| TestError(0));

            let mut pipe = pin!(
                tx.with_processor(Repeat { fail_on: None })
                    .with(|x: usize| async move { Ok::<_, TestError>(x) })
            );

            let mut send = pipe.send(2);

            assert!(futures::poll!(&mut send).is_pending());
            assert_eq!(rx.next().await, Some(2));
            assert!(futures::poll!(&mut send).is_pending());
            assert_eq!(rx.next().await, Some(2));
            assert!(futures::poll!(&mut send).is_ready());
        });
    }

    #[test]
    fn async_work() {
        struct Slow;

        impl Processor<usize> for Slow {
            type Output = usize;

            type Error = TestError;

            type Stream = stream::Once<futures::future::Ready<Result<usize, TestError>>>;

            type Future = Pin<Box<dyn Future<Output = Result<Self::Stream, TestError>>>>;

            fn process(&mut self, x: usize) -> Self::Future {
                Box::pin(async move {
                    tokio::time::sleep(Duration::from_millis(2)).await;
                    Ok(stream::once(futures::future::ready(Ok(x * 2))))
                })
            }
        }

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();

        let seen = Collector::default();

        rt.block_on(async {
            let mut pipe = pin!(collect(seen.clone()).with_processor(Slow));
            for i in 1..=3 {
                pipe.send(i).await.unwrap();
            }
            pipe.close().await.unwrap();
        });

        assert_eq!(*seen.borrow(), vec![2, 4, 6]);
    }
}
