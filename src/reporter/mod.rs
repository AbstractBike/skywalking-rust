// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

//! Reporter contains common `Report` trait and the implementations.

pub mod grpc;
#[cfg(feature = "kafka-reporter")]
pub mod kafka;
pub mod print;

#[cfg(feature = "management")]
use crate::proto::v3::{InstancePingPkg, InstanceProperties};
use crate::proto::v3::{LogData, MeterData, SegmentObject};
use serde::{Deserialize, Serialize};
use std::{error::Error, ops::Deref, sync::Arc};
use tokio::sync::{OnceCell, mpsc};
use tonic::async_trait;

/// Collect item of protobuf object.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[non_exhaustive]
pub enum CollectItem {
    /// Tracing object.
    Trace(Box<SegmentObject>),
    /// Log object.
    Log(Box<LogData>),
    /// Metric object.
    Meter(Box<MeterData>),
    /// Instance properties object.
    #[cfg(feature = "management")]
    Instance(Box<InstanceProperties>),
    /// Keep alive object.
    #[cfg(feature = "management")]
    Ping(Box<InstancePingPkg>),
}

impl CollectItem {
    #[cfg(feature = "kafka-reporter")]
    pub(crate) fn encode_to_vec(self) -> Vec<u8> {
        use prost::Message;

        match self {
            CollectItem::Trace(item) => item.encode_to_vec(),
            CollectItem::Log(item) => item.encode_to_vec(),
            CollectItem::Meter(item) => item.encode_to_vec(),
            #[cfg(feature = "management")]
            CollectItem::Instance(item) => item.encode_to_vec(),
            #[cfg(feature = "management")]
            CollectItem::Ping(item) => item.encode_to_vec(),
        }
    }
}

pub(crate) type DynReport = dyn Report + Send + Sync + 'static;

/// Report provide non-blocking report method for trace, log and metric object.
pub trait Report {
    /// The non-blocking report method.
    fn report(&self, item: CollectItem);
}

/// Noop reporter.
impl Report for () {
    fn report(&self, _item: CollectItem) {}
}

impl<T: Report> Report for Box<T> {
    fn report(&self, item: CollectItem) {
        Report::report(self.deref(), item)
    }
}

impl<T: Report> Report for Arc<T> {
    fn report(&self, item: CollectItem) {
        Report::report(self.deref(), item)
    }
}

impl<T: Report> Report for OnceCell<T> {
    fn report(&self, item: CollectItem) {
        Report::report(self.get().expect("OnceCell is empty"), item)
    }
}

/// Strategy when the internal bounded channel is full.
#[derive(Debug, Clone, Copy, Default)]
pub enum BackpressureStrategy {
    /// Drop the item and propagate the error to the error handler.
    #[default]
    Drop,
    /// Block the calling thread until space is available.
    Block,
}

/// Configuration for the internal channel used by reporters.
#[derive(Debug, Clone)]
pub struct ChannelConfig {
    /// Maximum number of items the channel can hold before applying backpressure.
    pub capacity: usize,
    /// Strategy applied when the channel is full.
    pub strategy: BackpressureStrategy,
}

impl Default for ChannelConfig {
    fn default() -> Self {
        Self {
            capacity: 1024,
            strategy: BackpressureStrategy::Drop,
        }
    }
}

/// A bounded mpsc sender with a configurable backpressure strategy.
pub struct BoundedSender {
    inner: mpsc::Sender<CollectItem>,
    strategy: BackpressureStrategy,
}

impl BoundedSender {
    pub(crate) fn new(inner: mpsc::Sender<CollectItem>, strategy: BackpressureStrategy) -> Self {
        Self { inner, strategy }
    }
}

/// Special purpose, used for user-defined production operations. Generally, it
/// does not need to be handled.
pub trait CollectItemProduce: Send + Sync + 'static {
    /// Produce the collect item non-blocking.
    fn produce(&self, item: CollectItem) -> Result<(), Box<dyn Error>>;
}

impl CollectItemProduce for () {
    fn produce(&self, _item: CollectItem) -> Result<(), Box<dyn Error>> {
        Ok(())
    }
}

impl CollectItemProduce for BoundedSender {
    fn produce(&self, item: CollectItem) -> Result<(), Box<dyn Error>> {
        match self.strategy {
            BackpressureStrategy::Drop => match self.inner.try_send(item) {
                Ok(()) => Ok(()),
                Err(e) => Err(Box::new(e)),
            },
            BackpressureStrategy::Block => Ok(self.inner.blocking_send(item)?),
        }
    }
}

impl CollectItemProduce for mpsc::Sender<CollectItem> {
    fn produce(&self, item: CollectItem) -> Result<(), Box<dyn Error>> {
        Ok(self.blocking_send(item)?)
    }
}

impl CollectItemProduce for mpsc::UnboundedSender<CollectItem> {
    fn produce(&self, item: CollectItem) -> Result<(), Box<dyn Error>> {
        Ok(self.send(item)?)
    }
}

/// Alias of method result of [CollectItemConsume].
pub type ConsumeResult = Result<Option<CollectItem>, Box<dyn Error + Send>>;

/// Special purpose, used for user-defined consume operations. Generally, it
/// does not need to be handled.
#[async_trait]
pub trait CollectItemConsume: Send + Sync + 'static {
    /// Consume the collect item blocking.
    async fn consume(&mut self) -> ConsumeResult;

    /// Try to consume the collect item non-blocking.
    async fn try_consume(&mut self) -> ConsumeResult;
}

#[async_trait]
impl CollectItemConsume for () {
    async fn consume(&mut self) -> ConsumeResult {
        Ok(None)
    }

    async fn try_consume(&mut self) -> ConsumeResult {
        Ok(None)
    }
}

#[async_trait]
impl CollectItemConsume for mpsc::Receiver<CollectItem> {
    async fn consume(&mut self) -> ConsumeResult {
        Ok(self.recv().await)
    }

    async fn try_consume(&mut self) -> ConsumeResult {
        use mpsc::error::TryRecvError;

        match self.try_recv() {
            Ok(item) => Ok(Some(item)),
            Err(e) => match e {
                TryRecvError::Empty => Ok(None),
                TryRecvError::Disconnected => Err(Box::new(e)),
            },
        }
    }
}

#[async_trait]
impl CollectItemConsume for mpsc::UnboundedReceiver<CollectItem> {
    async fn consume(&mut self) -> ConsumeResult {
        Ok(self.recv().await)
    }

    async fn try_consume(&mut self) -> ConsumeResult {
        use mpsc::error::TryRecvError;

        match self.try_recv() {
            Ok(item) => Ok(Some(item)),
            Err(e) => match e {
                TryRecvError::Empty => Ok(None),
                TryRecvError::Disconnected => Err(Box::new(e)),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::v3::SegmentObject;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    fn make_item() -> CollectItem {
        CollectItem::Trace(Box::new(SegmentObject::default()))
    }

    #[test]
    fn channel_config_default_capacity_is_1024() {
        let cfg = ChannelConfig::default();
        assert_eq!(cfg.capacity, 1024);
    }

    #[test]
    fn bounded_sender_drop_strategy_returns_err_when_full() {
        // Capacity of 1; fill it and verify produce returns Err on the second item.
        let (tx, _rx) = mpsc::channel::<CollectItem>(1);
        let sender = BoundedSender::new(tx, BackpressureStrategy::Drop);

        // First item fits.
        assert!(sender.produce(make_item()).is_ok());
        // Second item causes Full → Err propagated to err_handle.
        assert!(sender.produce(make_item()).is_err());
    }

    #[test]
    fn bounded_sender_drop_strategy_err_handle_called() {
        let (tx, _rx) = mpsc::channel::<CollectItem>(1);
        let sender = BoundedSender::new(tx, BackpressureStrategy::Drop);
        let counter = Arc::new(AtomicUsize::new(0));

        sender.produce(make_item()).ok();

        let c = counter.clone();
        // Simulate what GrpcReporter::report does: call err_handle on Err.
        if sender.produce(make_item()).is_err() {
            c.fetch_add(1, Ordering::SeqCst);
        }
        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn channel_config_custom_capacity() {
        let cfg = ChannelConfig {
            capacity: 512,
            strategy: BackpressureStrategy::Block,
        };
        let (tx, _rx) = mpsc::channel::<CollectItem>(cfg.capacity);
        assert_eq!(tx.capacity(), 512);
    }
}
