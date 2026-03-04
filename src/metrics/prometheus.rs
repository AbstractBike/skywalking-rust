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

//! Prometheus text-format exporter backed by [`super::recorder`].

use super::recorder::{MetricSnapshot, Registry};
use std::{
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::AsyncWriteExt,
    net::{TcpListener, TcpStream},
};

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Exports the registry as Prometheus text-format metrics.
///
/// Obtain one via [`SkyWalkingRecorderBuilder::with_prometheus_exporter`].
///
/// # Example
///
/// ```no_run
/// use skywalking::metrics::recorder::SkyWalkingRecorderBuilder;
///
/// # async fn example(reporter: impl skywalking::reporter::Report + Send + Sync + 'static) {
/// let (recorder, exporter) = SkyWalkingRecorderBuilder::new("svc", "inst")
///     .build_with_prometheus(reporter);
///
/// recorder.install_global().unwrap();
/// tokio::spawn(exporter.serve("0.0.0.0:9100".parse().unwrap()));
/// # }
/// ```
pub struct PrometheusExporter {
    pub(super) registry: Arc<Mutex<Registry>>,
}

impl PrometheusExporter {
    /// Render all metrics in Prometheus text exposition format.
    pub fn render(&self) -> String {
        let snapshots = self.registry.lock().unwrap().snapshots();
        let ts = now_ms();
        let mut out = String::new();

        for snap in snapshots {
            match snap {
                MetricSnapshot::Counter { name, labels, value } => {
                    let label_str = format_labels(&labels);
                    out.push_str(&format!(
                        "# HELP {name} Counter\n# TYPE {name} counter\n{name}{label_str} {value} {ts}\n"
                    ));
                }
                MetricSnapshot::Gauge { name, labels, value } => {
                    let label_str = format_labels(&labels);
                    out.push_str(&format!(
                        "# HELP {name} Gauge\n# TYPE {name} gauge\n{name}{label_str} {value} {ts}\n"
                    ));
                }
                MetricSnapshot::Histogram { name, labels, buckets } => {
                    out.push_str(&format!("# HELP {name} Histogram\n# TYPE {name} histogram\n"));
                    for bv in &buckets {
                        let le = bv.bucket;
                        let count = bv.count;
                        let lstr = format_labels_with_extra(&labels, "le", &le.to_string());
                        out.push_str(&format!("{name}_bucket{lstr} {count} {ts}\n"));
                    }
                    // +Inf bucket (total count)
                    let total: i64 = buckets.iter().map(|b| b.count).sum();
                    let inf_lstr = format_labels_with_extra(&labels, "le", "+Inf");
                    out.push_str(&format!("{name}_bucket{inf_lstr} {total} {ts}\n"));
                }
            }
        }

        out
    }

    /// Start a minimal HTTP server that serves `GET /metrics` at the given address.
    pub async fn serve(self, addr: SocketAddr) -> Result<(), Box<dyn std::error::Error + Send>> {
        let listener = TcpListener::bind(addr)
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send>)?;
        let exporter = Arc::new(self);

        loop {
            let (stream, _) = listener
                .accept()
                .await
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send>)?;
            let exp = exporter.clone();
            tokio::spawn(async move {
                let _ = handle_connection(stream, exp).await;
            });
        }
    }
}

async fn handle_connection(
    mut stream: TcpStream,
    exporter: Arc<PrometheusExporter>,
) -> std::io::Result<()> {
    // Minimal HTTP/1.1 request parsing — just enough to route GET /metrics.
    let mut buf = [0u8; 1024];
    let _ = stream.try_read(&mut buf);

    let body = exporter.render();
    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        body.len(),
        body
    );
    stream.write_all(response.as_bytes()).await?;
    stream.flush().await
}

fn format_labels(labels: &[crate::proto::v3::Label]) -> String {
    if labels.is_empty() {
        return String::new();
    }
    let pairs: Vec<String> = labels
        .iter()
        .map(|l| format!("{}=\"{}\"", l.name, l.value))
        .collect();
    format!("{{{}}}", pairs.join(","))
}

fn format_labels_with_extra(
    labels: &[crate::proto::v3::Label],
    extra_key: &str,
    extra_val: &str,
) -> String {
    let mut pairs: Vec<String> = labels
        .iter()
        .map(|l| format!("{}=\"{}\"", l.name, l.value))
        .collect();
    pairs.push(format!("{}=\"{}\"", extra_key, extra_val));
    format!("{{{}}}", pairs.join(","))
}

// ---------------------------------------------------------------------------
// Wire PrometheusExporter into the recorder builder
// ---------------------------------------------------------------------------

impl super::recorder::SkyWalkingRecorderBuilder {
    /// Build the recorder and a [`PrometheusExporter`] that share the same registry.
    pub fn build_with_prometheus(
        self,
        reporter: impl crate::reporter::Report + Send + Sync + 'static,
    ) -> (super::recorder::SkyWalkingRecorder, PrometheusExporter) {
        let rec = self.build(reporter);
        let exporter = PrometheusExporter {
            registry: rec.registry.clone(),
        };
        (rec, exporter)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::recorder::SkyWalkingRecorderBuilder;
    use crate::reporter::CollectItem;
    use metrics::{Key, Metadata, Recorder};
    use std::sync::{Arc, Mutex};

    #[derive(Default, Clone)]
    struct Cap {
        items: Arc<Mutex<Vec<CollectItem>>>,
    }
    impl crate::reporter::Report for Cap {
        fn report(&self, item: CollectItem) {
            self.items.lock().unwrap().push(item);
        }
    }

    fn make_exporter() -> (super::super::recorder::SkyWalkingRecorder, PrometheusExporter) {
        SkyWalkingRecorderBuilder::new("svc", "inst")
            .histogram_buckets(vec![1.0, 5.0, 10.0])
            .build_with_prometheus(Cap::default())
    }

    #[test]
    fn render_includes_counter_type_line() {
        let (rec, exp) = make_exporter();
        let key = Key::from_name("http_requests_total");
        let c = rec.register_counter(&key, &Metadata::new("", metrics::Level::INFO, None));
        c.increment(7);

        let output = exp.render();
        assert!(output.contains("# TYPE http_requests_total counter"), "missing TYPE line");
        assert!(output.contains("http_requests_total"), "missing metric line");
    }

    #[test]
    fn render_includes_gauge_type_line() {
        let (rec, exp) = make_exporter();
        let key = Key::from_name("memory_bytes");
        let g = rec.register_gauge(&key, &Metadata::new("", metrics::Level::INFO, None));
        g.set(4096.0);

        let output = exp.render();
        assert!(output.contains("# TYPE memory_bytes gauge"));
        assert!(output.contains("memory_bytes"));
    }

    #[test]
    fn render_includes_histogram_buckets() {
        let (rec, exp) = make_exporter();
        let key = Key::from_name("latency_ms");
        let h = rec.register_histogram(&key, &Metadata::new("", metrics::Level::INFO, None));
        h.record(0.5);
        h.record(3.0);

        let output = exp.render();
        assert!(output.contains("# TYPE latency_ms histogram"));
        assert!(output.contains("latency_ms_bucket{le=\"1\""), "missing le=1 bucket");
        assert!(output.contains("le=\"+Inf\""), "missing +Inf bucket");
    }
}
