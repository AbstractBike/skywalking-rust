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

//! [`metrics::Recorder`] implementation that forwards to SkyWalking.

use crate::{
    common::system_time::{TimePeriod, fetch_time},
    proto::v3::{Label, MeterBucketValue, MeterData, MeterHistogram, MeterSingleValue, meter_data::Metric},
    reporter::{CollectItem, Report},
};
use metrics::{
    Counter, CounterFn, Gauge, GaugeFn, Histogram, HistogramFn, Key, KeyName, Metadata, Recorder,
    SetRecorderError, SharedString, Unit,
};
use portable_atomic::AtomicF64;
use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicI64, Ordering},
    },
    time::Duration,
};

/// Default histogram bucket upper bounds (Prometheus HTTP latency defaults, in seconds).
const DEFAULT_BUCKETS: &[f64] = &[
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
];

// ---------------------------------------------------------------------------
// Backing types that implement the metrics-rs function traits
// ---------------------------------------------------------------------------

struct SwCounter {
    value: AtomicF64,
}

impl SwCounter {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            value: AtomicF64::new(0.0),
        })
    }

    fn get(&self) -> f64 {
        self.value.load(Ordering::Relaxed)
    }
}

impl CounterFn for SwCounter {
    fn increment(&self, value: u64) {
        self.value.fetch_add(value as f64, Ordering::Relaxed);
    }

    fn absolute(&self, value: u64) {
        self.value.store(value as f64, Ordering::Relaxed);
    }
}

struct SwGauge {
    value: AtomicF64,
}

impl SwGauge {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            value: AtomicF64::new(0.0),
        })
    }

    fn get(&self) -> f64 {
        self.value.load(Ordering::Relaxed)
    }
}

impl GaugeFn for SwGauge {
    fn increment(&self, value: f64) {
        self.value.fetch_add(value, Ordering::Relaxed);
    }

    fn decrement(&self, value: f64) {
        self.value.fetch_sub(value, Ordering::Relaxed);
    }

    fn set(&self, value: f64) {
        self.value.store(value, Ordering::Relaxed);
    }
}

struct SwHistogram {
    /// (upper_bound, count) pairs, sorted ascending.
    buckets: Vec<(f64, AtomicI64)>,
}

impl SwHistogram {
    fn new(bounds: &[f64]) -> Arc<Self> {
        let mut sorted = bounds.to_vec();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        sorted.dedup();
        Arc::new(Self {
            buckets: sorted.into_iter().map(|b| (b, AtomicI64::new(0))).collect(),
        })
    }

    fn snapshot_buckets(&self) -> Vec<MeterBucketValue> {
        self.buckets
            .iter()
            .map(|(bound, count)| MeterBucketValue {
                bucket: *bound,
                count: count.load(Ordering::Relaxed),
                is_negative_infinity: false,
            })
            .collect()
    }
}

impl HistogramFn for SwHistogram {
    fn record(&self, value: f64) {
        // Find the first bucket whose upper_bound >= value.
        let idx = self.buckets.partition_point(|(bound, _)| *bound < value);
        if idx < self.buckets.len() {
            self.buckets[idx].1.fetch_add(1, Ordering::Relaxed);
        }
        // Values above the largest bucket are silently dropped (same as SW SDK behaviour).
    }
}

// ---------------------------------------------------------------------------
// Registry — shared state between the recorder and the reporting task
// ---------------------------------------------------------------------------

/// Key type used in the registry maps.
type RegistryKey = String;

fn registry_key(key: &Key) -> RegistryKey {
    // Include labels in the key so distinct label sets get separate entries.
    let labels: Vec<String> = key
        .labels()
        .map(|l| format!("{}={}", l.key(), l.value()))
        .collect();
    if labels.is_empty() {
        key.name().to_string()
    } else {
        format!("{}[{}]", key.name(), labels.join(","))
    }
}

/// Snapshot of a single metric for reporting.
pub(super) enum MetricSnapshot {
    Counter {
        name: String,
        labels: Vec<Label>,
        value: f64,
    },
    Gauge {
        name: String,
        labels: Vec<Label>,
        value: f64,
    },
    Histogram {
        name: String,
        labels: Vec<Label>,
        buckets: Vec<MeterBucketValue>,
    },
}

impl MetricSnapshot {
    fn into_meter_data(self, service: &str, instance: &str) -> MeterData {
        let ts = fetch_time(TimePeriod::Metric);
        match self {
            MetricSnapshot::Counter { name, labels, value } => MeterData {
                service: service.to_owned(),
                service_instance: instance.to_owned(),
                timestamp: ts,
                metric: Some(Metric::SingleValue(MeterSingleValue { name, labels, value })),
            },
            MetricSnapshot::Gauge { name, labels, value } => MeterData {
                service: service.to_owned(),
                service_instance: instance.to_owned(),
                timestamp: ts,
                metric: Some(Metric::SingleValue(MeterSingleValue { name, labels, value })),
            },
            MetricSnapshot::Histogram { name, labels, buckets } => MeterData {
                service: service.to_owned(),
                service_instance: instance.to_owned(),
                timestamp: ts,
                metric: Some(Metric::Histogram(MeterHistogram { name, labels, values: buckets })),
            },
        }
    }
}

pub(super) struct Registry {
    counters: HashMap<RegistryKey, (Arc<SwCounter>, String, Vec<Label>)>,
    gauges: HashMap<RegistryKey, (Arc<SwGauge>, String, Vec<Label>)>,
    histograms: HashMap<RegistryKey, (Arc<SwHistogram>, String, Vec<Label>)>,
    histogram_buckets: Vec<f64>,
}

impl Registry {
    fn new(histogram_buckets: Vec<f64>) -> Self {
        Self {
            counters: HashMap::new(),
            gauges: HashMap::new(),
            histograms: HashMap::new(),
            histogram_buckets,
        }
    }

    fn labels_from_key(key: &Key) -> Vec<Label> {
        key.labels()
            .map(|l| Label {
                name: l.key().to_string(),
                value: l.value().to_string(),
            })
            .collect()
    }

    fn get_or_create_counter(&mut self, key: &Key) -> Arc<SwCounter> {
        let rkey = registry_key(key);
        self.counters
            .entry(rkey)
            .or_insert_with(|| {
                (
                    SwCounter::new(),
                    key.name().to_string(),
                    Self::labels_from_key(key),
                )
            })
            .0
            .clone()
    }

    fn get_or_create_gauge(&mut self, key: &Key) -> Arc<SwGauge> {
        let rkey = registry_key(key);
        self.gauges
            .entry(rkey)
            .or_insert_with(|| {
                (
                    SwGauge::new(),
                    key.name().to_string(),
                    Self::labels_from_key(key),
                )
            })
            .0
            .clone()
    }

    fn get_or_create_histogram(&mut self, key: &Key) -> Arc<SwHistogram> {
        let rkey = registry_key(key);
        let buckets = self.histogram_buckets.clone();
        self.histograms
            .entry(rkey)
            .or_insert_with(|| {
                (
                    SwHistogram::new(&buckets),
                    key.name().to_string(),
                    Self::labels_from_key(key),
                )
            })
            .0
            .clone()
    }

    pub(super) fn snapshots(&self) -> Vec<MetricSnapshot> {
        let mut out = Vec::new();

        for (_, (counter, name, labels)) in &self.counters {
            out.push(MetricSnapshot::Counter {
                name: name.clone(),
                labels: labels.clone(),
                value: counter.get(),
            });
        }

        for (_, (gauge, name, labels)) in &self.gauges {
            out.push(MetricSnapshot::Gauge {
                name: name.clone(),
                labels: labels.clone(),
                value: gauge.get(),
            });
        }

        for (_, (histogram, name, labels)) in &self.histograms {
            out.push(MetricSnapshot::Histogram {
                name: name.clone(),
                labels: labels.clone(),
                buckets: histogram.snapshot_buckets(),
            });
        }

        out
    }
}

// ---------------------------------------------------------------------------
// SkyWalkingRecorder
// ---------------------------------------------------------------------------

/// A [`metrics::Recorder`] that reports metrics to SkyWalking OAP.
///
/// # Example
///
/// ```no_run
/// use skywalking::metrics::recorder::SkyWalkingRecorderBuilder;
///
/// # async fn example(reporter: impl skywalking::reporter::Report + Send + Sync + 'static) {
/// SkyWalkingRecorderBuilder::new("my-service", "instance-1")
///     .build(reporter)
///     .install_global()
///     .unwrap();
///
/// // Now metrics macros report to SkyWalking.
/// metrics::counter!("http_requests_total", "method" => "GET").increment(1);
/// # }
/// ```
pub struct SkyWalkingRecorder {
    service: String,
    instance: String,
    reporter: Arc<dyn Report + Send + Sync>,
    pub(super) registry: Arc<Mutex<Registry>>,
}

impl SkyWalkingRecorder {
    /// Install this recorder as the global `metrics` recorder and spawn a background
    /// reporting task with the given interval.
    pub fn install_global(
        self,
    ) -> Result<ReportingHandle, SetRecorderError<SkyWalkingRecorder>> {
        // Spawn reporting using cloned Arc references before consuming self.
        let service = self.service.clone();
        let instance = self.instance.clone();
        let reporter = self.reporter.clone();
        let registry = self.registry.clone();

        metrics::set_global_recorder(self)?;

        let interval = Duration::from_secs(20);
        let handle = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            loop {
                ticker.tick().await;
                let snapshots = registry.lock().unwrap().snapshots();
                for snapshot in snapshots {
                    reporter.report(CollectItem::Meter(Box::new(
                        snapshot.into_meter_data(&service, &instance),
                    )));
                }
            }
        });

        Ok(ReportingHandle { handle })
    }

    /// Spawn a background tokio task that periodically reports all metrics.
    ///
    /// The recorder is consumed to avoid double-reporting when combined with
    /// [`install_global`](Self::install_global). Use [`install_global`] if you
    /// also need the recorder installed as the global.
    pub fn spawn_reporting(self, interval: Duration) -> ReportingHandle {
        let service = self.service;
        let instance = self.instance;
        let reporter = self.reporter;
        let registry = self.registry;

        let handle = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            loop {
                ticker.tick().await;
                let snapshots = registry.lock().unwrap().snapshots();
                for snapshot in snapshots {
                    reporter.report(CollectItem::Meter(Box::new(
                        snapshot.into_meter_data(&service, &instance),
                    )));
                }
            }
        });

        ReportingHandle { handle }
    }
}

impl Recorder for SkyWalkingRecorder {
    fn describe_counter(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}
    fn describe_gauge(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}
    fn describe_histogram(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

    fn register_counter(&self, key: &Key, _metadata: &Metadata<'_>) -> Counter {
        let inner = self.registry.lock().unwrap().get_or_create_counter(key);
        Counter::from_arc(inner)
    }

    fn register_gauge(&self, key: &Key, _metadata: &Metadata<'_>) -> Gauge {
        let inner = self.registry.lock().unwrap().get_or_create_gauge(key);
        Gauge::from_arc(inner)
    }

    fn register_histogram(&self, key: &Key, _metadata: &Metadata<'_>) -> Histogram {
        let inner = self.registry.lock().unwrap().get_or_create_histogram(key);
        Histogram::from_arc(inner)
    }
}

// ---------------------------------------------------------------------------
// Handle
// ---------------------------------------------------------------------------

/// Handle returned by [`SkyWalkingRecorder::spawn_reporting`].
///
/// Can be awaited to run indefinitely or dropped to cancel.
pub struct ReportingHandle {
    handle: tokio::task::JoinHandle<()>,
}

impl ReportingHandle {
    /// Abort the background reporting task.
    pub fn abort(&self) {
        self.handle.abort();
    }
}

// ---------------------------------------------------------------------------
// Builder
// ---------------------------------------------------------------------------

/// Builder for [`SkyWalkingRecorder`].
pub struct SkyWalkingRecorderBuilder {
    service: String,
    instance: String,
    histogram_buckets: Vec<f64>,
}

impl SkyWalkingRecorderBuilder {
    /// Create a new builder.
    pub fn new(service: impl Into<String>, instance: impl Into<String>) -> Self {
        Self {
            service: service.into(),
            instance: instance.into(),
            histogram_buckets: DEFAULT_BUCKETS.to_vec(),
        }
    }

    /// Override the default histogram bucket upper bounds.
    pub fn histogram_buckets(mut self, bounds: impl Into<Vec<f64>>) -> Self {
        self.histogram_buckets = bounds.into();
        self
    }

    /// Build the [`SkyWalkingRecorder`] with the provided reporter.
    pub fn build(self, reporter: impl Report + Send + Sync + 'static) -> SkyWalkingRecorder {
        SkyWalkingRecorder {
            service: self.service,
            instance: self.instance,
            reporter: Arc::new(reporter),
            registry: Arc::new(Mutex::new(Registry::new(self.histogram_buckets))),
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reporter::CollectItem;
    use std::sync::{Arc, Mutex};

    #[derive(Default, Clone)]
    struct Cap {
        items: Arc<Mutex<Vec<CollectItem>>>,
    }

    impl Report for Cap {
        fn report(&self, item: CollectItem) {
            self.items.lock().unwrap().push(item);
        }
    }

    fn recorder(cap: Cap) -> SkyWalkingRecorder {
        SkyWalkingRecorderBuilder::new("svc", "inst").build(cap)
    }

    #[test]
    fn counter_increments_are_tracked() {
        let cap = Cap::default();
        let rec = recorder(cap);

        let key = Key::from_name("requests_total");
        let c = rec.register_counter(&key, &Metadata::new("", metrics::Level::INFO, None));
        c.increment(5);
        c.increment(3);

        let snapshots = rec.registry.lock().unwrap().snapshots();
        let snap = snapshots
            .iter()
            .find(|s| matches!(s, MetricSnapshot::Counter { name, .. } if name == "requests_total"));
        assert!(snap.is_some(), "counter snapshot missing");
        if let Some(MetricSnapshot::Counter { value, .. }) = snap {
            assert_eq!(*value, 8.0);
        }
    }

    #[test]
    fn gauge_set_reflects_current_value() {
        let cap = Cap::default();
        let rec = recorder(cap);

        let key = Key::from_name("memory_bytes");
        let g = rec.register_gauge(&key, &Metadata::new("", metrics::Level::INFO, None));
        g.set(1024.0);
        g.increment(256.0);
        g.decrement(128.0);

        let snapshots = rec.registry.lock().unwrap().snapshots();
        let snap = snapshots
            .iter()
            .find(|s| matches!(s, MetricSnapshot::Gauge { name, .. } if name == "memory_bytes"));
        assert!(snap.is_some());
        if let Some(MetricSnapshot::Gauge { value, .. }) = snap {
            assert_eq!(*value, 1152.0);
        }
    }

    #[test]
    fn histogram_records_into_correct_bucket() {
        let cap = Cap::default();
        let rec = SkyWalkingRecorderBuilder::new("svc", "inst")
            .histogram_buckets(vec![1.0, 5.0, 10.0])
            .build(cap);

        let key = Key::from_name("latency_ms");
        let h = rec.register_histogram(&key, &Metadata::new("", metrics::Level::INFO, None));
        h.record(0.5); // → bucket 1.0
        h.record(3.0); // → bucket 5.0
        h.record(3.5); // → bucket 5.0

        let snapshots = rec.registry.lock().unwrap().snapshots();
        let snap = snapshots
            .iter()
            .find(|s| matches!(s, MetricSnapshot::Histogram { name, .. } if name == "latency_ms"));
        assert!(snap.is_some());
        if let Some(MetricSnapshot::Histogram { buckets, .. }) = snap {
            let b1 = buckets.iter().find(|b| b.bucket == 1.0).unwrap();
            let b5 = buckets.iter().find(|b| b.bucket == 5.0).unwrap();
            assert_eq!(b1.count, 1, "bucket 1.0 should have 1 observation");
            assert_eq!(b5.count, 2, "bucket 5.0 should have 2 observations");
        }
    }
}
