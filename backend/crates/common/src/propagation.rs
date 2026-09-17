use std::collections::BTreeMap;

use opentelemetry::propagation::{Extractor, Injector};
use opentelemetry::Context;
use tracing_opentelemetry::OpenTelemetrySpanExt;

struct HeaderInjector<'a>(&'a mut BTreeMap<String, Vec<u8>>);

impl Injector for HeaderInjector<'_> {
    fn set(&mut self, key: &str, value: String) {
        self.0.insert(key.to_string(), value.into_bytes());
    }
}

struct HeaderExtractor<'a>(&'a BTreeMap<String, Vec<u8>>);

impl Extractor for HeaderExtractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(|v| std::str::from_utf8(v).ok())
    }

    fn keys(&self) -> Vec<&str> {
        self.0.keys().map(String::as_str).collect()
    }
}

/// Injects the current tracing span's context into Kafka message headers.
pub fn inject(headers: &mut BTreeMap<String, Vec<u8>>) {
    let cx = tracing::Span::current().context();
    opentelemetry::global::get_text_map_propagator(|propagator| {
        propagator.inject_context(&cx, &mut HeaderInjector(headers));
    });
}

/// Extracts a parent context from Kafka message headers, for use with
/// `tracing_opentelemetry::OpenTelemetrySpanExt::set_parent`.
pub fn extract(headers: &BTreeMap<String, Vec<u8>>) -> Context {
    opentelemetry::global::get_text_map_propagator(|propagator| {
        propagator.extract(&HeaderExtractor(headers))
    })
}
