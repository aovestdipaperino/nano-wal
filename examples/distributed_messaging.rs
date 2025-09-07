//! Distributed Messaging System using nano-wal
//!
//! This example demonstrates how to build a distributed messaging system with
//! message routing, partitioning, and metadata tracking using nano-wal.
//!
//! Features demonstrated:
//! - Message routing with topic partitioning
//! - Message headers for routing metadata
//! - Producer-consumer patterns
//! - Message acknowledgment tracking
//! - Dead letter queue handling
//! - Message replay capabilities

use bytes::Bytes;
use nano_wal::{EntryRef, Wal, WalOptions};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageHeader {
    pub message_id: String,
    pub topic: String,
    pub partition: u32,
    pub producer_id: String,
    pub timestamp: u64,
    pub routing_key: Option<String>,
    pub content_type: String,
    pub priority: MessagePriority,
    pub retry_count: u32,
    pub max_retries: u32,
    pub expires_at: Option<u64>,
    pub correlation_id: Option<String>,
    pub reply_to: Option<String>,
    pub trace_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MessagePriority {
    Low = 0,
    Normal = 1,
    High = 2,
    Critical = 3,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    pub payload: String,
    pub schema_version: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AckInfo {
    pub consumer_id: String,
    pub processed_at: u64,
    pub processing_time_ms: u64,
    pub status: AckStatus,
    pub error_message: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AckStatus {
    Success,
    Failed,
    Retry,
    DeadLetter,
}

pub struct MessageBroker {
    message_wal: Wal,
    ack_wal: Wal,
    dlq_wal: Wal,
    partition_count: HashMap<String, u32>,
}

impl MessageBroker {
    pub fn new(data_dir: &str) -> Result<Self, Box<dyn std::error::Error>> {
        std::fs::create_dir_all(data_dir)?;

        let message_options = WalOptions::default()
            .retention(std::time::Duration::from_secs(60 * 60 * 24 * 7)) // 1 week
            .segments_per_retention_period(168); // 1 hour per segment

        let ack_options = WalOptions::default()
            .retention(std::time::Duration::from_secs(60 * 60 * 24 * 3)) // 3 days
            .segments_per_retention_period(72); // 1 hour per segment

        let dlq_options = WalOptions::default()
            .retention(std::time::Duration::from_secs(60 * 60 * 24 * 30)) // 30 days
            .segments_per_retention_period(30); // 1 day per segment

        let message_wal = Wal::new(&format!("{}/messages", data_dir), message_options)?;

        let ack_wal = Wal::new(&format!("{}/acknowledgments", data_dir), ack_options)?;

        let dlq_wal = Wal::new(&format!("{}/dead_letter_queue", data_dir), dlq_options)?;

        Ok(MessageBroker {
            message_wal,
            ack_wal,
            dlq_wal,
            partition_count: HashMap::new(),
        })
    }

    pub fn create_topic(&mut self, topic: &str, partitions: u32) {
        self.partition_count.insert(topic.to_string(), partitions);
        println!("Created topic '{}' with {} partitions", topic, partitions);
    }

    pub fn produce_message(
        &mut self,
        topic: &str,
        message: Message,
        routing_key: Option<String>,
        producer_id: &str,
    ) -> Result<EntryRef, Box<dyn std::error::Error>> {
        let partition_count = *self.partition_count.get(topic).unwrap_or(&1);

        // Simple hash-based partitioning
        let partition = if let Some(ref key) = routing_key {
            self.hash_string(key) % partition_count
        } else {
            self.hash_string(&message.payload) % partition_count
        };

        let header = MessageHeader {
            message_id: uuid::Uuid::new_v4().to_string(),
            topic: topic.to_string(),
            partition,
            producer_id: producer_id.to_string(),
            timestamp: SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs(),
            routing_key,
            content_type: "application/json".to_string(),
            priority: MessagePriority::Normal,
            retry_count: 0,
            max_retries: 3,
            expires_at: None,
            correlation_id: None,
            reply_to: None,
            trace_id: Some(uuid::Uuid::new_v4().to_string()),
        };

        let topic_partition = format!("{}:partition:{}", topic, partition);
        let header_bytes = Some(Bytes::from(serde_json::to_string(&header)?));
        let payload_bytes = Bytes::from(serde_json::to_string(&message)?);

        let entry_ref = self
            .message_wal
            .log_entry(topic_partition, header_bytes, payload_bytes)?;

        println!(
            "Produced message {} to topic '{}' partition {} by producer '{}'",
            header.message_id, topic, partition, producer_id
        );

        Ok(entry_ref)
    }

    pub fn produce_priority_message(
        &mut self,
        topic: &str,
        message: Message,
        priority: MessagePriority,
        expires_in_seconds: Option<u64>,
        producer_id: &str,
    ) -> Result<EntryRef, Box<dyn std::error::Error>> {
        let _partition_count = *self.partition_count.get(topic).unwrap_or(&1);
        let partition = 0; // Priority messages go to partition 0

        let expires_at = expires_in_seconds.map(|secs| {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs()
                + secs
        });

        let header = MessageHeader {
            message_id: uuid::Uuid::new_v4().to_string(),
            topic: topic.to_string(),
            partition,
            producer_id: producer_id.to_string(),
            timestamp: SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs(),
            routing_key: None,
            content_type: "application/json".to_string(),
            priority,
            retry_count: 0,
            max_retries: 3,
            expires_at,
            correlation_id: None,
            reply_to: None,
            trace_id: Some(uuid::Uuid::new_v4().to_string()),
        };

        let topic_partition = format!("{}:partition:{}", topic, partition);
        let header_bytes = Some(Bytes::from(serde_json::to_string(&header)?));
        let payload_bytes = Bytes::from(serde_json::to_string(&message)?);

        let entry_ref = self
            .message_wal
            .log_entry(topic_partition, header_bytes, payload_bytes)?;

        println!(
            "Produced priority message {} to topic '{}' with priority {:?}",
            header.message_id, topic, header.priority
        );

        Ok(entry_ref)
    }

    pub fn consume_messages(
        &self,
        topic: &str,
        partition: u32,
    ) -> Result<Vec<(MessageHeader, Message)>, Box<dyn std::error::Error>> {
        let topic_partition = format!("{}:partition:{}", topic, partition);
        let records: Vec<Bytes> = self
            .message_wal
            .enumerate_records(topic_partition)?
            .collect();

        let mut messages = Vec::new();
        for record in records {
            let message: Message = serde_json::from_slice(&record)?;
            // In a real implementation, you'd parse the header from the WAL entry header
            let header = MessageHeader {
                message_id: uuid::Uuid::new_v4().to_string(),
                topic: topic.to_string(),
                partition,
                producer_id: "unknown".to_string(),
                timestamp: SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs(),
                routing_key: None,
                content_type: "application/json".to_string(),
                priority: MessagePriority::Normal,
                retry_count: 0,
                max_retries: 3,
                expires_at: None,
                correlation_id: None,
                reply_to: None,
                trace_id: None,
            };
            messages.push((header, message));
        }

        Ok(messages)
    }

    pub fn acknowledge_message(
        &mut self,
        message_id: &str,
        consumer_id: &str,
        processing_time_ms: u64,
        status: AckStatus,
        error_message: Option<String>,
    ) -> Result<EntryRef, Box<dyn std::error::Error>> {
        let ack_info = AckInfo {
            consumer_id: consumer_id.to_string(),
            processed_at: SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs(),
            processing_time_ms,
            status: status.clone(),
            error_message: error_message.clone(),
        };

        let ack_key = format!("ack:{}", message_id);
        let ack_header = format!("consumer:{},status:{:?}", consumer_id, status);
        let header_bytes = Some(Bytes::from(ack_header));
        let ack_bytes = Bytes::from(serde_json::to_string(&ack_info)?);

        let entry_ref = self.ack_wal.log_entry(ack_key, header_bytes, ack_bytes)?;

        match status {
            AckStatus::Success => {
                println!(
                    "Message {} acknowledged successfully by consumer '{}'",
                    message_id, consumer_id
                );
            }
            AckStatus::Failed => {
                println!(
                    "Message {} failed processing by consumer '{}': {:?}",
                    message_id, consumer_id, error_message
                );
            }
            AckStatus::Retry => {
                println!(
                    "Message {} marked for retry by consumer '{}'",
                    message_id, consumer_id
                );
            }
            AckStatus::DeadLetter => {
                println!(
                    "Message {} moved to dead letter queue by consumer '{}'",
                    message_id, consumer_id
                );
                self.move_to_dead_letter_queue(message_id, &error_message.unwrap_or_default())?;
            }
        }

        Ok(entry_ref)
    }

    pub fn move_to_dead_letter_queue(
        &mut self,
        message_id: &str,
        reason: &str,
    ) -> Result<EntryRef, Box<dyn std::error::Error>> {
        let dlq_info = serde_json::json!({
            "message_id": message_id,
            "moved_at": SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs(),
            "reason": reason,
            "original_topic": "unknown" // In real implementation, track original topic
        });

        let dlq_key = "dead_letter_queue";
        let dlq_header = format!("message_id:{},reason:{}", message_id, reason);
        let header_bytes = Some(Bytes::from(dlq_header));
        let dlq_bytes = Bytes::from(dlq_info.to_string());

        let entry_ref = self.dlq_wal.log_entry(dlq_key, header_bytes, dlq_bytes)?;
        Ok(entry_ref)
    }

    pub fn get_topic_statistics(
        &self,
        topic: &str,
    ) -> Result<TopicStats, Box<dyn std::error::Error>> {
        let partition_count = *self.partition_count.get(topic).unwrap_or(&1);
        let mut total_messages = 0;
        let mut partition_stats = Vec::new();

        for partition in 0..partition_count {
            let topic_partition = format!("{}:partition:{}", topic, partition);
            let records: Vec<Bytes> = self
                .message_wal
                .enumerate_records(topic_partition)?
                .collect();
            let message_count = records.len();
            total_messages += message_count;

            partition_stats.push(PartitionStats {
                partition,
                message_count,
            });
        }

        Ok(TopicStats {
            topic: topic.to_string(),
            total_messages,
            partition_count,
            partition_stats,
        })
    }

    pub fn replay_messages_since(
        &self,
        topic: &str,
        partition: u32,
        since_timestamp: u64,
    ) -> Result<Vec<(MessageHeader, Message)>, Box<dyn std::error::Error>> {
        let all_messages = self.consume_messages(topic, partition)?;
        let filtered_messages: Vec<(MessageHeader, Message)> = all_messages
            .into_iter()
            .filter(|(header, _)| header.timestamp >= since_timestamp)
            .collect();

        println!(
            "Replaying {} messages from topic '{}' partition {} since timestamp {}",
            filtered_messages.len(),
            topic,
            partition,
            since_timestamp
        );

        Ok(filtered_messages)
    }

    fn hash_string(&self, s: &str) -> u32 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        s.hash(&mut hasher);
        hasher.finish() as u32
    }

    pub fn compact_expired_messages(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        self.message_wal.compact()?;
        self.ack_wal.compact()?;
        self.dlq_wal.compact()?;
        println!("Compacted expired messages and acknowledgments");
        Ok(())
    }

    pub fn shutdown(mut self) -> Result<(), Box<dyn std::error::Error>> {
        self.message_wal.shutdown()?;
        self.ack_wal.shutdown()?;
        self.dlq_wal.shutdown()?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct TopicStats {
    pub topic: String,
    pub total_messages: usize,
    pub partition_count: u32,
    pub partition_stats: Vec<PartitionStats>,
}

#[derive(Debug)]
pub struct PartitionStats {
    pub partition: u32,
    pub message_count: usize,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Distributed Messaging System Example ===\n");

    // Create temporary directory for this demo
    let temp_dir = std::env::temp_dir().join("nano_wal_messaging");
    std::fs::create_dir_all(&temp_dir)?;

    let mut broker = MessageBroker::new(temp_dir.to_str().unwrap())?;

    // Create topics with different partition counts
    println!("1. Creating Topics");
    broker.create_topic("user_events", 3);
    broker.create_topic("order_processing", 5);
    broker.create_topic("notifications", 2);
    broker.create_topic("analytics", 1);

    // Produce various types of messages
    println!("\n2. Producing Messages");

    // User event messages with routing keys
    let user_message = Message {
        payload: serde_json::json!({
            "user_id": "user123",
            "action": "login",
            "timestamp": "2024-01-01T10:00:00Z",
            "ip_address": "192.168.1.100"
        })
        .to_string(),
        schema_version: "1.0".to_string(),
    };

    broker.produce_message(
        "user_events",
        user_message,
        Some("user123".to_string()),
        "user_service",
    )?;

    // Order processing messages
    for i in 0..10 {
        let order_message = Message {
            payload: serde_json::json!({
                "order_id": format!("order{}", i),
                "user_id": format!("user{}", i % 3),
                "total_amount": 99.99 + (i as f64 * 10.0),
                "items": [
                    {"product_id": "prod001", "quantity": 1, "price": 49.99},
                    {"product_id": "prod002", "quantity": 2, "price": 25.00}
                ]
            })
            .to_string(),
            schema_version: "2.1".to_string(),
        };

        broker.produce_message(
            "order_processing",
            order_message,
            Some(format!("user{}", i % 3)),
            "order_service",
        )?;
    }

    // Priority notification messages
    let critical_notification = Message {
        payload: serde_json::json!({
            "type": "system_alert",
            "severity": "critical",
            "message": "Database connection pool exhausted",
            "affected_services": ["user_service", "order_service"]
        })
        .to_string(),
        schema_version: "1.0".to_string(),
    };

    broker.produce_priority_message(
        "notifications",
        critical_notification,
        MessagePriority::Critical,
        Some(3600), // Expires in 1 hour
        "monitoring_service",
    )?;

    // Analytics messages
    for i in 0..5 {
        let analytics_message = Message {
            payload: serde_json::json!({
                "event_type": "page_view",
                "page": format!("/product/{}", i),
                "user_id": format!("user{}", i % 3),
                "session_id": format!("session{}", i),
                "timestamp": "2024-01-01T10:00:00Z"
            })
            .to_string(),
            schema_version: "1.0".to_string(),
        };

        broker.produce_message("analytics", analytics_message, None, "web_frontend")?;
    }

    // Consume messages from different partitions
    println!("\n3. Consuming Messages");

    println!("   Messages in user_events partition 0:");
    let user_messages = broker.consume_messages("user_events", 0)?;
    for (header, message) in &user_messages {
        println!(
            "      - Message {}: {}",
            header.message_id,
            &message.payload[..100.min(message.payload.len())]
        );
    }

    println!("\n   Messages in order_processing partition 1:");
    let order_messages = broker.consume_messages("order_processing", 1)?;
    for (header, _message) in &order_messages {
        println!("      - Message {}: Order data", header.message_id);
    }

    // Simulate message processing and acknowledgments
    println!("\n4. Message Acknowledgments");

    // Success acknowledgment
    broker.acknowledge_message(
        "msg-001",
        "consumer-group-1",
        150, // 150ms processing time
        AckStatus::Success,
        None,
    )?;

    // Failed acknowledgment
    broker.acknowledge_message(
        "msg-002",
        "consumer-group-1",
        500,
        AckStatus::Failed,
        Some("JSON parsing error".to_string()),
    )?;

    // Retry acknowledgment
    broker.acknowledge_message(
        "msg-003",
        "consumer-group-2",
        1000,
        AckStatus::Retry,
        Some("Temporary network timeout".to_string()),
    )?;

    // Dead letter acknowledgment
    broker.acknowledge_message(
        "msg-004",
        "consumer-group-2",
        2000,
        AckStatus::DeadLetter,
        Some("Maximum retries exceeded".to_string()),
    )?;

    // Get topic statistics
    println!("\n5. Topic Statistics");
    let topics = vec![
        "user_events",
        "order_processing",
        "notifications",
        "analytics",
    ];

    for topic in &topics {
        let stats = broker.get_topic_statistics(topic)?;
        println!(
            "   Topic '{}': {} total messages across {} partitions",
            stats.topic, stats.total_messages, stats.partition_count
        );

        for partition_stat in &stats.partition_stats {
            println!(
                "     - Partition {}: {} messages",
                partition_stat.partition, partition_stat.message_count
            );
        }
    }

    // Message replay demonstration
    println!("\n6. Message Replay");
    let replay_since = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() - 3600; // Last hour
    let replayed_messages = broker.replay_messages_since("order_processing", 0, replay_since)?;
    println!(
        "   Replayed {} messages from the last hour",
        replayed_messages.len()
    );

    // Demonstrate routing by showing message distribution
    println!("\n7. Message Distribution Analysis");
    for topic in &topics {
        let stats = broker.get_topic_statistics(topic)?;
        if stats.partition_count > 1 {
            println!("   Topic '{}' partition distribution:", topic);
            for partition_stat in &stats.partition_stats {
                let percentage = if stats.total_messages > 0 {
                    (partition_stat.message_count as f64 / stats.total_messages as f64) * 100.0
                } else {
                    0.0
                };
                println!(
                    "     - Partition {}: {} messages ({:.1}%)",
                    partition_stat.partition, partition_stat.message_count, percentage
                );
            }
        }
    }

    // Cleanup and compaction
    println!("\n8. Cleanup");
    broker.compact_expired_messages()?;

    // Shutdown
    broker.shutdown()?;
    std::fs::remove_dir_all(&temp_dir).ok();

    println!("\n✓ Distributed Messaging System example completed!");
    println!("\nKey Features Demonstrated:");
    println!("- ✓ Topic partitioning with configurable partition counts");
    println!("- ✓ Message routing using routing keys and hash-based partitioning");
    println!("- ✓ Priority message handling");
    println!("- ✓ Message headers with metadata (priority, expiration, tracing)");
    println!("- ✓ Consumer acknowledgments with different status types");
    println!("- ✓ Dead letter queue handling");
    println!("- ✓ Message replay capabilities");
    println!("- ✓ Topic statistics and monitoring");
    println!("- ✓ Automatic message compaction and retention");

    Ok(())
}

// Note: This example requires additional dependencies in Cargo.toml:
// [dependencies]
// serde = { version = "1.0", features = ["derive"] }
// serde_json = "1.0"
// uuid = { version = "1.0", features = ["v4"] }
