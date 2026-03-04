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

//! [`tracing_subscriber::Layer`] implementation for SkyWalking.

use crate::{
    common::{
        random_generator::RandomGenerator,
        system_time::{TimePeriod, fetch_time},
    },
    proto::v3::{KeyStringValuePair, Log, SegmentObject, SpanLayer, SpanObject, SpanType},
    reporter::{CollectItem, Report},
};
use std::sync::{
    Arc,
    atomic::{AtomicI32, Ordering},
};
use tracing::field::{Field, Visit};
use tracing_subscriber::{Layer, layer::Context, registry::LookupSpan};

// ---------------------------------------------------------------------------
// Per-span extension data
// ---------------------------------------------------------------------------

/// Data stored in every span's extensions.
struct SpanData {
    span_object: SpanObject,
    /// `Some(id)` when this span is a child; points to the root span.
    root_span_id: Option<tracing::span::Id>,
}

/// Data stored only in the root span's extensions.
struct RootData {
    trace_id: String,
    trace_segment_id: String,
    /// Finalized child `SpanObject`s accumulated here until the root closes.
    finalized_children: Vec<SpanObject>,
}

// ---------------------------------------------------------------------------
// SkyWalkingLayer
// ---------------------------------------------------------------------------

/// A [`tracing_subscriber::Layer`] that converts `tracing` spans into
/// SkyWalking [`SegmentObject`]s and reports them via the provided reporter.
///
/// Each root `tracing` span becomes one SkyWalking segment. Child spans are
/// collected as [`SpanObject`]s inside the same segment and submitted when the
/// root span closes.
///
/// # Example
///
/// ```no_run
/// use skywalking::trace::layer::SkyWalkingLayerBuilder;
/// use tracing_subscriber::prelude::*;
///
/// # async fn example(reporter: impl skywalking::reporter::Report + Send + Sync + 'static) {
/// let layer = SkyWalkingLayerBuilder::new("my-service", "instance-1")
///     .build(reporter);
///
/// tracing_subscriber::registry().with(layer).init();
/// # }
/// ```
pub struct SkyWalkingLayer {
    service: String,
    instance: String,
    reporter: Arc<dyn Report + Send + Sync>,
    next_span_id: AtomicI32,
}

impl SkyWalkingLayer {
    fn next_span_id(&self) -> i32 {
        self.next_span_id.fetch_add(1, Ordering::Relaxed)
    }
}

impl<S> Layer<S> for SkyWalkingLayer
where
    S: tracing::Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(
        &self,
        attrs: &tracing::span::Attributes<'_>,
        id: &tracing::span::Id,
        ctx: Context<'_, S>,
    ) {
        let span = match ctx.span(id) {
            Some(s) => s,
            None => return,
        };

        let sw_span_id = self.next_span_id();

        // Determine parent relationship.
        let (span_type, parent_span_id, root_span_id) = span
            .parent()
            .and_then(|p| {
                let p_ext = p.extensions();
                let p_data = p_ext.get::<SpanData>()?;
                let parent_sw_id = p_data.span_object.span_id;
                let root_id = p_data
                    .root_span_id
                    .clone()
                    .unwrap_or_else(|| p.id().clone());
                Some((SpanType::Local, parent_sw_id, Some(root_id)))
            })
            .unwrap_or((SpanType::Entry, -1, None));

        let mut span_object = SpanObject {
            span_id: sw_span_id,
            parent_span_id,
            start_time: fetch_time(TimePeriod::Start),
            end_time: 0,
            operation_name: attrs.metadata().name().to_string(),
            peer: String::new(),
            span_type: span_type as i32,
            span_layer: SpanLayer::Unknown as i32,
            component_id: 0,
            is_error: false,
            tags: Vec::new(),
            logs: Vec::new(),
            refs: Vec::new(),
            skip_analysis: false,
        };

        // Collect initial field values as tags.
        let mut visitor = TagVisitor::default();
        attrs.record(&mut visitor);
        span_object.tags.extend(visitor.tags);

        let mut ext = span.extensions_mut();

        // Root span: attach RootData.
        if root_span_id.is_none() {
            ext.insert(RootData {
                trace_id: RandomGenerator::generate(),
                trace_segment_id: RandomGenerator::generate(),
                finalized_children: Vec::new(),
            });
        }

        ext.insert(SpanData {
            span_object,
            root_span_id,
        });
    }

    fn on_record(
        &self,
        id: &tracing::span::Id,
        values: &tracing::span::Record<'_>,
        ctx: Context<'_, S>,
    ) {
        let span = match ctx.span(id) {
            Some(s) => s,
            None => return,
        };
        let mut ext = span.extensions_mut();
        if let Some(data) = ext.get_mut::<SpanData>() {
            let mut visitor = TagVisitor::default();
            values.record(&mut visitor);
            data.span_object.tags.extend(visitor.tags);
        }
    }

    fn on_event(&self, event: &tracing::Event<'_>, ctx: Context<'_, S>) {
        if let Some(span) = ctx.lookup_current() {
            let mut ext = span.extensions_mut();
            if let Some(data) = ext.get_mut::<SpanData>() {
                let mut visitor = LogVisitor::default();
                event.record(&mut visitor);
                if !visitor.fields.is_empty() {
                    data.span_object.logs.push(Log {
                        time: fetch_time(TimePeriod::Log),
                        data: visitor.fields,
                    });
                }
            }
        }
    }

    fn on_close(&self, id: tracing::span::Id, ctx: Context<'_, S>) {
        let span = match ctx.span(&id) {
            Some(s) => s,
            None => return,
        };

        // Extract per-span data. Drop `ext` before accessing the root span to
        // avoid holding two extension locks simultaneously.
        let (mut span_object, root_span_id) = {
            let mut ext = span.extensions_mut();
            match ext.remove::<SpanData>() {
                Some(data) => (data.span_object, data.root_span_id),
                None => return,
            }
        };

        span_object.end_time = fetch_time(TimePeriod::End);

        if let Some(root_id) = root_span_id {
            // Child span: move SpanObject into the root's collection.
            if let Some(root_span) = ctx.span(&root_id) {
                let mut root_ext = root_span.extensions_mut();
                if let Some(root_data) = root_ext.get_mut::<RootData>() {
                    root_data.finalized_children.push(span_object);
                }
            }
        } else {
            // Root span: assemble and report the SegmentObject.
            let root_data = {
                let mut ext = span.extensions_mut();
                ext.remove::<RootData>()
            };

            if let Some(root_data) = root_data {
                let mut spans = root_data.finalized_children;
                spans.push(span_object);
                spans.sort_by_key(|s| s.span_id);

                let segment = SegmentObject {
                    trace_id: root_data.trace_id,
                    trace_segment_id: root_data.trace_segment_id,
                    spans,
                    service: self.service.clone(),
                    service_instance: self.instance.clone(),
                    is_size_limited: false,
                };

                self.reporter.report(CollectItem::Trace(Box::new(segment)));
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Builder
// ---------------------------------------------------------------------------

/// Builder for [`SkyWalkingLayer`].
pub struct SkyWalkingLayerBuilder {
    service: String,
    instance: String,
}

impl SkyWalkingLayerBuilder {
    /// Create a new builder.
    pub fn new(service: impl Into<String>, instance: impl Into<String>) -> Self {
        Self {
            service: service.into(),
            instance: instance.into(),
        }
    }

    /// Build the [`SkyWalkingLayer`] with the provided reporter.
    pub fn build(self, reporter: impl Report + Send + Sync + 'static) -> SkyWalkingLayer {
        SkyWalkingLayer {
            service: self.service,
            instance: self.instance,
            reporter: Arc::new(reporter),
            next_span_id: AtomicI32::new(0),
        }
    }
}

// ---------------------------------------------------------------------------
// Field visitors
// ---------------------------------------------------------------------------

#[derive(Default)]
struct TagVisitor {
    tags: Vec<KeyStringValuePair>,
}

impl Visit for TagVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.tags.push(KeyStringValuePair {
            key: field.name().to_string(),
            value: format!("{value:?}"),
        });
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        self.tags.push(KeyStringValuePair {
            key: field.name().to_string(),
            value: value.to_string(),
        });
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        self.tags.push(KeyStringValuePair {
            key: field.name().to_string(),
            value: value.to_string(),
        });
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        self.tags.push(KeyStringValuePair {
            key: field.name().to_string(),
            value: value.to_string(),
        });
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.tags.push(KeyStringValuePair {
            key: field.name().to_string(),
            value: value.to_string(),
        });
    }
}

#[derive(Default)]
struct LogVisitor {
    fields: Vec<KeyStringValuePair>,
}

impl Visit for LogVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.fields.push(KeyStringValuePair {
            key: field.name().to_string(),
            value: format!("{value:?}"),
        });
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        self.fields.push(KeyStringValuePair {
            key: field.name().to_string(),
            value: value.to_string(),
        });
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        self.fields.push(KeyStringValuePair {
            key: field.name().to_string(),
            value: value.to_string(),
        });
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        self.fields.push(KeyStringValuePair {
            key: field.name().to_string(),
            value: value.to_string(),
        });
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.fields.push(KeyStringValuePair {
            key: field.name().to_string(),
            value: value.to_string(),
        });
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
    use tracing::subscriber::with_default;
    use tracing_subscriber::prelude::*;

    /// Captures reported items for assertions.
    #[derive(Default, Clone)]
    struct CapturingReporter {
        items: Arc<Mutex<Vec<CollectItem>>>,
    }

    impl Report for CapturingReporter {
        fn report(&self, item: CollectItem) {
            self.items.lock().unwrap().push(item);
        }
    }

    fn make_subscriber(reporter: CapturingReporter) -> impl tracing::Subscriber {
        let layer = SkyWalkingLayerBuilder::new("test-svc", "test-inst").build(reporter);
        tracing_subscriber::registry().with(layer)
    }

    #[test]
    fn root_span_produces_one_segment_with_entry_span() {
        let reporter = CapturingReporter::default();
        let items = reporter.items.clone();
        let sub = make_subscriber(reporter);

        with_default(sub, || {
            let _span = tracing::info_span!("root_op").entered();
        });

        let items = items.lock().unwrap();
        assert_eq!(items.len(), 1, "expected exactly one segment");
        let CollectItem::Trace(seg) = &items[0] else {
            panic!("expected Trace item");
        };
        assert_eq!(seg.service, "test-svc");
        assert_eq!(seg.service_instance, "test-inst");
        assert_eq!(seg.spans.len(), 1);
        assert_eq!(seg.spans[0].span_type, SpanType::Entry as i32);
        assert_eq!(seg.spans[0].operation_name, "root_op");
        assert_eq!(seg.spans[0].parent_span_id, -1);
    }

    #[test]
    fn child_span_becomes_local_span_in_same_segment() {
        let reporter = CapturingReporter::default();
        let items = reporter.items.clone();
        let sub = make_subscriber(reporter);

        with_default(sub, || {
            let root = tracing::info_span!("root").entered();
            {
                let _child = tracing::info_span!("child").entered();
            }
            drop(root);
        });

        let items = items.lock().unwrap();
        assert_eq!(items.len(), 1);
        let CollectItem::Trace(seg) = &items[0] else {
            panic!("expected Trace item");
        };
        assert_eq!(seg.spans.len(), 2, "expected root + child");
        // Sorted by span_id: root first (lower id), child second.
        let entry = seg.spans.iter().find(|s| s.span_type == SpanType::Entry as i32);
        let local = seg.spans.iter().find(|s| s.span_type == SpanType::Local as i32);
        assert!(entry.is_some(), "no Entry span found");
        assert!(local.is_some(), "no Local span found");
        assert_eq!(local.unwrap().operation_name, "child");
    }

    #[test]
    fn event_in_span_becomes_log_entry() {
        let reporter = CapturingReporter::default();
        let items = reporter.items.clone();
        let sub = make_subscriber(reporter);

        with_default(sub, || {
            let _root = tracing::info_span!("root").entered();
            tracing::info!(message = "hello", code = 42u64);
        });

        let items = items.lock().unwrap();
        let CollectItem::Trace(seg) = &items[0] else {
            panic!("expected Trace item");
        };
        let span = &seg.spans[0];
        assert_eq!(span.logs.len(), 1, "expected one log entry");
        assert!(
            span.logs[0]
                .data
                .iter()
                .any(|kv| kv.key == "message" && kv.value == "hello"),
            "log should contain message=hello"
        );
    }
}
