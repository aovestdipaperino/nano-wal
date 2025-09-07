//! Real-time Analytics Pipeline using nano-wal
//!
//! This example demonstrates how to build a real-time analytics system using nano-wal
//! for event ingestion, processing, and metrics calculation.
//!
//! Features demonstrated:
//! - High-frequency event ingestion
//! - Real-time metrics calculation
//! - Time-window aggregations
//! - Event deduplication using headers
//! - Multi-stream processing
//! - Performance monitoring

use bytes::Bytes;
use nano_wal::{EntryRef, Wal, WalOptions};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventHeader {
    pub event_id: String,
    pub source: String,
    pub timestamp: u64,
    pub event_type: String,
    pub session_id: Option<String>,
    pub user_id: Option<String>,
    pub ip_address: Option<String>,
    pub user_agent: Option<String>,
    pub dedup_key: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum AnalyticsEvent {
    PageView {
        page_url: String,
        page_title: String,
        referrer: Option<String>,
        load_time_ms: u32,
    },
    UserAction {
        action: String,
        target: String,
        properties: HashMap<String, String>,
    },
    Purchase {
        transaction_id: String,
        items: Vec<PurchaseItem>,
        total_amount: f64,
        currency: String,
        payment_method: String,
    },
    Error {
        error_code: String,
        error_message: String,
        stack_trace: Option<String>,
        severity: ErrorSeverity,
    },
    Performance {
        metric_name: String,
        value: f64,
        unit: String,
        tags: HashMap<String, String>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PurchaseItem {
    pub product_id: String,
    pub product_name: String,
    pub quantity: u32,
    pub price: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ErrorSeverity {
    Info,
    Warning,
    Error,
    Critical,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Metrics {
    pub page_views: u64,
    pub unique_users: u64,
    pub total_revenue: f64,
    pub error_count: u64,
    pub avg_load_time: f64,
    pub top_pages: HashMap<String, u32>,
    pub error_by_severity: HashMap<String, u32>,
}

impl Default for Metrics {
    fn default() -> Self {
        Self {
            page_views: 0,
            unique_users: 0,
            total_revenue: 0.0,
            error_count: 0,
            avg_load_time: 0.0,
            top_pages: HashMap::new(),
            error_by_severity: HashMap::new(),
        }
    }
}

pub struct AnalyticsPipeline {
    events_wal: Wal,
    metrics_wal: Wal,
    dedup_cache: HashMap<String, u64>,
    unique_users: HashMap<String, bool>,
    total_load_time: f64,
    load_time_count: u64,
}

impl AnalyticsPipeline {
    pub fn new(data_dir: &str) -> Result<Self, Box<dyn std::error::Error>> {
        std::fs::create_dir_all(data_dir)?;

        // High-frequency events with shorter retention
        let events_options = WalOptions::default()
            .retention(Duration::from_secs(60 * 60 * 24 * 7)) // 1 week
            .segments_per_retention_period(168); // 1 hour per segment

        // Aggregated metrics with longer retention
        let metrics_options = WalOptions::default()
            .retention(Duration::from_secs(60 * 60 * 24 * 90)) // 90 days
            .segments_per_retention_period(90); // 1 day per segment

        let events_wal = Wal::new(&format!("{}/events", data_dir), events_options)?;
        let metrics_wal = Wal::new(&format!("{}/metrics", data_dir), metrics_options)?;

        Ok(Self {
            events_wal,
            metrics_wal,
            dedup_cache: HashMap::new(),
            unique_users: HashMap::new(),
            total_load_time: 0.0,
            load_time_count: 0,
        })
    }

    pub fn ingest_event(
        &mut self,
        event: AnalyticsEvent,
        source: &str,
        session_id: Option<String>,
        user_id: Option<String>,
        ip_address: Option<String>,
        user_agent: Option<String>,
    ) -> Result<Option<EntryRef>, Box<dyn std::error::Error>> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();

        // Create deduplication key for certain event types
        let dedup_key = match &event {
            AnalyticsEvent::Purchase { transaction_id, .. } => {
                Some(format!("purchase:{}", transaction_id))
            }
            AnalyticsEvent::Error { error_code, .. } => {
                Some(format!("error:{}:{}", error_code, now / 60)) // Dedupe same error per minute
            }
            _ => None,
        };

        // Check for duplicates
        if let Some(ref key) = dedup_key {
            if self.dedup_cache.contains_key(key) {
                println!("Duplicate event detected, skipping: {}", key);
                return Ok(None);
            }
            self.dedup_cache.insert(key.clone(), now);
        }

        let header = EventHeader {
            event_id: uuid::Uuid::new_v4().to_string(),
            source: source.to_string(),
            timestamp: now,
            event_type: self.get_event_type(&event),
            session_id,
            user_id: user_id.clone(),
            ip_address,
            user_agent,
            dedup_key,
        };

        // Determine stream based on event type
        let stream_key = match &event {
            AnalyticsEvent::PageView { .. } => "page_views",
            AnalyticsEvent::UserAction { .. } => "user_actions",
            AnalyticsEvent::Purchase { .. } => "purchases",
            AnalyticsEvent::Error { .. } => "errors",
            AnalyticsEvent::Performance { .. } => "performance",
        };

        let header_bytes = Some(Bytes::from(serde_json::to_string(&header)?));
        let event_bytes = Bytes::from(serde_json::to_string(&event)?);

        let entry_ref = self
            .events_wal
            .log_entry(stream_key, header_bytes, event_bytes)?;

        // Update real-time metrics
        self.update_metrics(&header, &event)?;

        println!(
            "Ingested {} event from {} (ID: {})",
            stream_key, source, header.event_id
        );

        Ok(Some(entry_ref))
    }

    fn update_metrics(
        &mut self,
        header: &EventHeader,
        event: &AnalyticsEvent,
    ) -> Result<(), Box<dyn std::error::Error>> {
        match event {
            AnalyticsEvent::PageView {
                page_url,
                load_time_ms,
                ..
            } => {
                // Track unique users
                if let Some(ref user_id) = header.user_id {
                    self.unique_users.insert(user_id.clone(), true);
                }

                // Update load time average
                self.total_load_time += *load_time_ms as f64;
                self.load_time_count += 1;

                println!(
                    "   → Page view: {} (load time: {}ms)",
                    page_url, load_time_ms
                );
            }
            AnalyticsEvent::Purchase { total_amount, .. } => {
                println!("   → Purchase: ${:.2}", total_amount);
            }
            AnalyticsEvent::Error { severity, .. } => {
                println!("   → Error: {:?} severity", severity);
            }
            AnalyticsEvent::UserAction { action, target, .. } => {
                println!("   → User action: {} on {}", action, target);
            }
            AnalyticsEvent::Performance {
                metric_name, value, ..
            } => {
                println!("   → Performance metric: {} = {}", metric_name, value);
            }
        }

        Ok(())
    }

    pub fn calculate_metrics(&self) -> Result<Metrics, Box<dyn std::error::Error>> {
        let mut metrics = Metrics::default();

        // Page views
        let page_view_records: Vec<Bytes> =
            self.events_wal.enumerate_records("page_views")?.collect();
        metrics.page_views = page_view_records.len() as u64;

        // Unique users
        metrics.unique_users = self.unique_users.len() as u64;

        // Average load time
        metrics.avg_load_time = if self.load_time_count > 0 {
            self.total_load_time / self.load_time_count as f64
        } else {
            0.0
        };

        // Calculate revenue from purchases
        let purchase_records: Vec<Bytes> =
            self.events_wal.enumerate_records("purchases")?.collect();
        for record in &purchase_records {
            if let Ok(event) = serde_json::from_slice::<AnalyticsEvent>(&record) {
                if let AnalyticsEvent::Purchase { total_amount, .. } = event {
                    metrics.total_revenue += total_amount;
                }
            }
        }

        // Error count
        let error_records: Vec<Bytes> = self.events_wal.enumerate_records("errors")?.collect();
        metrics.error_count = error_records.len() as u64;

        // Top pages
        for record in &page_view_records {
            if let Ok(event) = serde_json::from_slice::<AnalyticsEvent>(&record) {
                if let AnalyticsEvent::PageView { page_url, .. } = event {
                    *metrics.top_pages.entry(page_url).or_insert(0) += 1;
                }
            }
        }

        // Error by severity
        for record in &error_records {
            if let Ok(event) = serde_json::from_slice::<AnalyticsEvent>(&record) {
                if let AnalyticsEvent::Error { severity, .. } = event {
                    let severity_str = format!("{:?}", severity);
                    *metrics.error_by_severity.entry(severity_str).or_insert(0) += 1;
                }
            }
        }

        Ok(metrics)
    }

    pub fn store_metrics_snapshot(&mut self) -> Result<EntryRef, Box<dyn std::error::Error>> {
        let metrics = self.calculate_metrics()?;
        let timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();

        let snapshot_header = format!("snapshot_time:{}", timestamp);
        let header_bytes = Some(Bytes::from(snapshot_header));
        let metrics_bytes = Bytes::from(serde_json::to_string(&metrics)?);

        let entry_ref =
            self.metrics_wal
                .log_entry("metrics_snapshots", header_bytes, metrics_bytes)?;

        println!("Stored metrics snapshot at timestamp {}", timestamp);
        Ok(entry_ref)
    }

    pub fn get_event_counts_by_type(
        &self,
    ) -> Result<HashMap<String, usize>, Box<dyn std::error::Error>> {
        let mut counts = HashMap::new();

        let streams = vec![
            "page_views",
            "user_actions",
            "purchases",
            "errors",
            "performance",
        ];

        for stream in streams {
            let records: Vec<Bytes> = self.events_wal.enumerate_records(stream)?.collect();
            counts.insert(stream.to_string(), records.len());
        }

        Ok(counts)
    }

    pub fn get_recent_events(
        &self,
        stream: &str,
        limit: usize,
    ) -> Result<Vec<(EventHeader, AnalyticsEvent)>, Box<dyn std::error::Error>> {
        let records: Vec<Bytes> = self.events_wal.enumerate_records(stream)?.collect();

        let mut events = Vec::new();
        for record in records.iter().rev().take(limit) {
            if let Ok(event) = serde_json::from_slice::<AnalyticsEvent>(&record) {
                // In a real implementation, you'd parse the header from the WAL entry
                let header = EventHeader {
                    event_id: uuid::Uuid::new_v4().to_string(),
                    source: "unknown".to_string(),
                    timestamp: SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs(),
                    event_type: self.get_event_type(&event),
                    session_id: None,
                    user_id: None,
                    ip_address: None,
                    user_agent: None,
                    dedup_key: None,
                };
                events.push((header, event));
            }
        }

        Ok(events)
    }

    fn get_event_type(&self, event: &AnalyticsEvent) -> String {
        match event {
            AnalyticsEvent::PageView { .. } => "page_view".to_string(),
            AnalyticsEvent::UserAction { .. } => "user_action".to_string(),
            AnalyticsEvent::Purchase { .. } => "purchase".to_string(),
            AnalyticsEvent::Error { .. } => "error".to_string(),
            AnalyticsEvent::Performance { .. } => "performance".to_string(),
        }
    }

    pub fn cleanup_old_dedup_cache(&mut self, max_age_seconds: u64) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        self.dedup_cache
            .retain(|_, &mut timestamp| now - timestamp < max_age_seconds);
    }

    pub fn compact_old_data(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        self.events_wal.compact()?;
        self.metrics_wal.compact()?;
        println!("Compacted old analytics data");
        Ok(())
    }

    pub fn shutdown(mut self) -> Result<(), Box<dyn std::error::Error>> {
        self.events_wal.shutdown()?;
        self.metrics_wal.shutdown()?;
        Ok(())
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Real-time Analytics Pipeline Example ===\n");

    // Create temporary directory for this demo
    let temp_dir = std::env::temp_dir().join("nano_wal_analytics");
    std::fs::create_dir_all(&temp_dir)?;

    let mut analytics = AnalyticsPipeline::new(temp_dir.to_str().unwrap())?;

    println!("1. Simulating High-Frequency Events");

    // Simulate page view events
    let pages = vec![
        "/home",
        "/products",
        "/product/123",
        "/cart",
        "/checkout",
        "/profile",
        "/about",
    ];

    for i in 0..50 {
        let page = pages[i % pages.len()];
        let user_id = format!("user_{}", i % 10); // 10 unique users
        let session_id = format!("session_{}", i % 20); // 20 sessions

        let page_view = AnalyticsEvent::PageView {
            page_url: page.to_string(),
            page_title: format!("Page {}", page),
            referrer: if i > 0 {
                Some("https://google.com".to_string())
            } else {
                None
            },
            load_time_ms: 100 + (i as u32 * 10) % 500, // Varying load times
        };

        analytics.ingest_event(
            page_view,
            "web_frontend",
            Some(session_id),
            Some(user_id),
            Some(format!("192.168.1.{}", i % 100 + 1)),
            Some("Mozilla/5.0 (compatible)".to_string()),
        )?;

        // Simulate some user actions
        if i % 5 == 0 {
            let mut properties = HashMap::new();
            properties.insert("button_text".to_string(), "Add to Cart".to_string());
            properties.insert("product_id".to_string(), "prod_123".to_string());

            let user_action = AnalyticsEvent::UserAction {
                action: "click".to_string(),
                target: "add_to_cart_button".to_string(),
                properties,
            };

            analytics.ingest_event(
                user_action,
                "web_frontend",
                Some(format!("session_{}", i % 20)),
                Some(format!("user_{}", i % 10)),
                None,
                None,
            )?;
        }
    }

    // Simulate purchase events
    println!("\n2. Processing Purchase Events");
    for i in 0..5 {
        let items = vec![PurchaseItem {
            product_id: format!("prod_{}", i),
            product_name: format!("Product {}", i),
            quantity: 1 + (i % 3),
            price: 29.99 + (i as f64 * 10.0),
        }];

        let purchase = AnalyticsEvent::Purchase {
            transaction_id: format!("txn_{}", i),
            items: items.clone(),
            total_amount: items
                .iter()
                .map(|item| item.price * item.quantity as f64)
                .sum(),
            currency: "USD".to_string(),
            payment_method: if i % 2 == 0 {
                "credit_card".to_string()
            } else {
                "paypal".to_string()
            },
        };

        analytics.ingest_event(
            purchase,
            "payment_service",
            Some(format!("session_{}", i)),
            Some(format!("user_{}", i)),
            None,
            None,
        )?;
    }

    // Simulate error events
    println!("\n3. Logging Error Events");
    let errors = vec![
        ("AUTH001", "Invalid credentials", ErrorSeverity::Warning),
        ("DB002", "Connection timeout", ErrorSeverity::Error),
        ("SYS003", "Out of memory", ErrorSeverity::Critical),
        ("API004", "Rate limit exceeded", ErrorSeverity::Info),
    ];

    for (i, (code, message, severity)) in errors.iter().enumerate() {
        let error_event = AnalyticsEvent::Error {
            error_code: code.to_string(),
            error_message: message.to_string(),
            stack_trace: Some(format!("Stack trace for error {}", i)),
            severity: severity.clone(),
        };

        analytics.ingest_event(
            error_event,
            "backend_service",
            None,
            None,
            Some("10.0.0.1".to_string()),
            None,
        )?;
    }

    // Simulate performance metrics
    println!("\n4. Collecting Performance Metrics");
    let performance_metrics = vec![
        ("cpu_usage", 45.2, "percent"),
        ("memory_usage", 78.5, "percent"),
        ("response_time", 250.0, "milliseconds"),
        ("throughput", 1250.0, "requests_per_second"),
    ];

    for (metric_name, value, unit) in performance_metrics {
        let mut tags = HashMap::new();
        tags.insert("service".to_string(), "web_api".to_string());
        tags.insert("environment".to_string(), "production".to_string());

        let perf_event = AnalyticsEvent::Performance {
            metric_name: metric_name.to_string(),
            value,
            unit: unit.to_string(),
            tags,
        };

        analytics.ingest_event(perf_event, "monitoring_agent", None, None, None, None)?;
    }

    // Calculate and display real-time metrics
    println!("\n5. Real-time Metrics Dashboard");
    let metrics = analytics.calculate_metrics()?;

    println!("   📊 Analytics Summary:");
    println!("     • Page Views: {}", metrics.page_views);
    println!("     • Unique Users: {}", metrics.unique_users);
    println!("     • Total Revenue: ${:.2}", metrics.total_revenue);
    println!("     • Error Count: {}", metrics.error_count);
    println!("     • Avg Load Time: {:.1}ms", metrics.avg_load_time);

    println!("\n   🔥 Top Pages:");
    let mut top_pages: Vec<_> = metrics.top_pages.iter().collect();
    top_pages.sort_by(|a, b| b.1.cmp(a.1));
    for (page, count) in top_pages.iter().take(5) {
        println!("     • {} ({} views)", page, count);
    }

    println!("\n   ⚠️  Errors by Severity:");
    for (severity, count) in &metrics.error_by_severity {
        println!("     • {}: {} errors", severity, count);
    }

    // Show event distribution
    println!("\n6. Event Distribution");
    let event_counts = analytics.get_event_counts_by_type()?;
    for (event_type, count) in &event_counts {
        println!("   {} events: {}", event_type, count);
    }

    // Store metrics snapshot
    println!("\n7. Storing Metrics Snapshot");
    analytics.store_metrics_snapshot()?;

    // Show recent events
    println!("\n8. Recent Error Events");
    let recent_errors = analytics.get_recent_events("errors", 3)?;
    for (header, event) in recent_errors {
        if let AnalyticsEvent::Error {
            error_code,
            error_message,
            severity,
            ..
        } = event
        {
            println!(
                "   • [{}] {} - {} ({})",
                header.timestamp,
                error_code,
                error_message,
                format!("{:?}", severity)
            );
        }
    }

    // Demonstrate deduplication
    println!("\n9. Testing Event Deduplication");
    let duplicate_purchase = AnalyticsEvent::Purchase {
        transaction_id: "txn_0".to_string(), // Duplicate transaction ID
        items: vec![],
        total_amount: 100.0,
        currency: "USD".to_string(),
        payment_method: "credit_card".to_string(),
    };

    // This should be skipped due to deduplication
    analytics.ingest_event(
        duplicate_purchase,
        "payment_service",
        None,
        None,
        None,
        None,
    )?;

    // Cleanup and final stats
    println!("\n10. Cleanup and Final Statistics");
    analytics.cleanup_old_dedup_cache(3600); // Clean cache older than 1 hour
    analytics.compact_old_data()?;

    let _final_metrics = analytics.calculate_metrics()?;
    println!("   Final event counts:");
    let final_counts = analytics.get_event_counts_by_type()?;
    for (event_type, count) in &final_counts {
        println!("     • {}: {} events", event_type, count);
    }

    // Shutdown
    analytics.shutdown()?;
    std::fs::remove_dir_all(&temp_dir).ok();

    println!("\n✓ Real-time Analytics Pipeline example completed!");
    println!("\nKey Features Demonstrated:");
    println!("- ✓ High-frequency event ingestion with different event types");
    println!("- ✓ Real-time metrics calculation and aggregation");
    println!("- ✓ Event deduplication using headers");
    println!("- ✓ Multi-stream processing (page views, purchases, errors, etc.)");
    println!("- ✓ Performance monitoring and alerting");
    println!("- ✓ Metrics snapshots for historical analysis");
    println!("- ✓ Recent event retrieval for debugging");
    println!("- ✓ Automatic data retention and compaction");

    Ok(())
}

// Note: This example requires additional dependencies in Cargo.toml:
// [dependencies]
// serde = { version = "1.0", features = ["derive"] }
// serde_json = "1.0"
// uuid = { version = "1.0", features = ["v4"] }
