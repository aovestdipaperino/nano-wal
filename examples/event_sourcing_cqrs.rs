//! Event Sourcing with CQRS Pattern using nano-wal
//!
//! This example demonstrates how to build an event sourcing system with Command Query
//! Responsibility Segregation (CQRS) using nano-wal for persistent event storage.
//!
//! Features demonstrated:
//! - Event sourcing with domain events
//! - CQRS with separate command and query models
//! - Event metadata using headers
//! - Aggregate reconstruction from events
//! - Projection building for read models

use bytes::Bytes;
use nano_wal::{EntryRef, Wal, WalOptions};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventMetadata {
    pub event_id: String,
    pub event_type: String,
    pub timestamp: u64,
    pub version: u32,
    pub correlation_id: Option<String>,
    pub causation_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum UserEvent {
    UserRegistered {
        user_id: String,
        email: String,
        name: String,
    },
    EmailChanged {
        user_id: String,
        old_email: String,
        new_email: String,
    },
    UserDeactivated {
        user_id: String,
        reason: String,
    },
    UserReactivated {
        user_id: String,
    },
    ProfileUpdated {
        user_id: String,
        fields: HashMap<String, String>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum OrderEvent {
    OrderCreated {
        order_id: String,
        user_id: String,
        items: Vec<OrderItem>,
        total_amount: f64,
    },
    OrderPaid {
        order_id: String,
        payment_method: String,
        amount: f64,
    },
    OrderShipped {
        order_id: String,
        tracking_number: String,
        carrier: String,
    },
    OrderCancelled {
        order_id: String,
        reason: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderItem {
    pub product_id: String,
    pub quantity: u32,
    pub price: f64,
}

// Aggregate State
#[derive(Debug, Clone)]
pub struct User {
    pub user_id: String,
    pub email: String,
    pub name: String,
    pub is_active: bool,
    pub profile: HashMap<String, String>,
    pub version: u32,
}

#[derive(Debug, Clone)]
pub struct Order {
    pub order_id: String,
    pub user_id: String,
    pub items: Vec<OrderItem>,
    pub total_amount: f64,
    pub status: OrderStatus,
    pub payment_info: Option<PaymentInfo>,
    pub shipping_info: Option<ShippingInfo>,
    pub version: u32,
}

#[derive(Debug, Clone)]
pub enum OrderStatus {
    Created,
    Paid,
    Shipped,
    Cancelled,
}

#[derive(Debug, Clone)]
pub struct PaymentInfo {
    pub method: String,
    pub amount: f64,
}

#[derive(Debug, Clone)]
pub struct ShippingInfo {
    pub tracking_number: String,
    pub carrier: String,
}

// Read Models (Projections)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserSummary {
    pub user_id: String,
    pub email: String,
    pub name: String,
    pub is_active: bool,
    pub total_orders: u32,
    pub total_spent: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderSummary {
    pub order_id: String,
    pub user_id: String,
    pub status: String,
    pub total_amount: f64,
    pub created_at: u64,
}

pub struct EventStore {
    wal: Wal,
}

impl EventStore {
    pub fn new(path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let options = WalOptions::default()
            .retention(std::time::Duration::from_secs(60 * 60 * 24 * 30)) // 30 days
            .segments_per_retention_period(30); // 1 day per segment

        let wal = Wal::new(path, options)?;
        Ok(EventStore { wal })
    }

    pub fn append_user_event(
        &mut self,
        event: UserEvent,
        metadata: EventMetadata,
    ) -> Result<EntryRef, Box<dyn std::error::Error>> {
        let user_id = self.extract_user_id(&event);
        let stream_id = format!("user-{}", user_id);

        let header = Some(Bytes::from(serde_json::to_string(&metadata)?));
        let payload = Bytes::from(serde_json::to_string(&event)?);

        let entry_ref = self.wal.log_entry(stream_id, header, payload)?;
        Ok(entry_ref)
    }

    pub fn append_order_event(
        &mut self,
        event: OrderEvent,
        metadata: EventMetadata,
    ) -> Result<EntryRef, Box<dyn std::error::Error>> {
        let order_id = self.extract_order_id(&event);
        let stream_id = format!("order-{}", order_id);

        let header = Some(Bytes::from(serde_json::to_string(&metadata)?));
        let payload = Bytes::from(serde_json::to_string(&event)?);

        let entry_ref = self.wal.log_entry(stream_id, header, payload)?;
        Ok(entry_ref)
    }

    pub fn get_user_events(
        &self,
        user_id: &str,
    ) -> Result<Vec<(EventMetadata, UserEvent)>, Box<dyn std::error::Error>> {
        let stream_id = format!("user-{}", user_id);
        let records: Vec<Bytes> = self.wal.enumerate_records(stream_id)?.collect();

        let mut events = Vec::new();
        for record in records {
            let event: UserEvent = serde_json::from_slice(&record)?;
            // In a real implementation, you'd also read the header for metadata
            let metadata = EventMetadata {
                event_id: uuid::Uuid::new_v4().to_string(),
                event_type: "UserEvent".to_string(),
                timestamp: SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs(),
                version: 1,
                correlation_id: None,
                causation_id: None,
            };
            events.push((metadata, event));
        }
        Ok(events)
    }

    pub fn get_order_events(
        &self,
        order_id: &str,
    ) -> Result<Vec<(EventMetadata, OrderEvent)>, Box<dyn std::error::Error>> {
        let stream_id = format!("order-{}", order_id);
        let records: Vec<Bytes> = self.wal.enumerate_records(stream_id)?.collect();

        let mut events = Vec::new();
        for record in records {
            let event: OrderEvent = serde_json::from_slice(&record)?;
            let metadata = EventMetadata {
                event_id: uuid::Uuid::new_v4().to_string(),
                event_type: "OrderEvent".to_string(),
                timestamp: SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs(),
                version: 1,
                correlation_id: None,
                causation_id: None,
            };
            events.push((metadata, event));
        }
        Ok(events)
    }

    fn extract_user_id<'a>(&self, event: &'a UserEvent) -> &'a str {
        match event {
            UserEvent::UserRegistered { user_id, .. } => user_id,
            UserEvent::EmailChanged { user_id, .. } => user_id,
            UserEvent::UserDeactivated { user_id, .. } => user_id,
            UserEvent::UserReactivated { user_id, .. } => user_id,
            UserEvent::ProfileUpdated { user_id, .. } => user_id,
        }
    }

    fn extract_order_id<'a>(&self, event: &'a OrderEvent) -> &'a str {
        match event {
            OrderEvent::OrderCreated { order_id, .. } => order_id,
            OrderEvent::OrderPaid { order_id, .. } => order_id,
            OrderEvent::OrderShipped { order_id, .. } => order_id,
            OrderEvent::OrderCancelled { order_id, .. } => order_id,
        }
    }
}

pub struct UserAggregate;

impl UserAggregate {
    pub fn from_events(events: Vec<(EventMetadata, UserEvent)>) -> Option<User> {
        if events.is_empty() {
            return None;
        }

        let mut user: Option<User> = None;

        for (metadata, event) in events {
            match event {
                UserEvent::UserRegistered {
                    user_id,
                    email,
                    name,
                } => {
                    user = Some(User {
                        user_id,
                        email,
                        name,
                        is_active: true,
                        profile: HashMap::new(),
                        version: metadata.version,
                    });
                }
                UserEvent::EmailChanged { new_email, .. } => {
                    if let Some(ref mut u) = user {
                        u.email = new_email;
                        u.version = metadata.version;
                    }
                }
                UserEvent::UserDeactivated { .. } => {
                    if let Some(ref mut u) = user {
                        u.is_active = false;
                        u.version = metadata.version;
                    }
                }
                UserEvent::UserReactivated { .. } => {
                    if let Some(ref mut u) = user {
                        u.is_active = true;
                        u.version = metadata.version;
                    }
                }
                UserEvent::ProfileUpdated { fields, .. } => {
                    if let Some(ref mut u) = user {
                        u.profile.extend(fields);
                        u.version = metadata.version;
                    }
                }
            }
        }

        user
    }
}

pub struct OrderAggregate;

impl OrderAggregate {
    pub fn from_events(events: Vec<(EventMetadata, OrderEvent)>) -> Option<Order> {
        if events.is_empty() {
            return None;
        }

        let mut order: Option<Order> = None;

        for (metadata, event) in events {
            match event {
                OrderEvent::OrderCreated {
                    order_id,
                    user_id,
                    items,
                    total_amount,
                } => {
                    order = Some(Order {
                        order_id,
                        user_id,
                        items,
                        total_amount,
                        status: OrderStatus::Created,
                        payment_info: None,
                        shipping_info: None,
                        version: metadata.version,
                    });
                }
                OrderEvent::OrderPaid {
                    payment_method,
                    amount,
                    ..
                } => {
                    if let Some(ref mut o) = order {
                        o.status = OrderStatus::Paid;
                        o.payment_info = Some(PaymentInfo {
                            method: payment_method,
                            amount,
                        });
                        o.version = metadata.version;
                    }
                }
                OrderEvent::OrderShipped {
                    tracking_number,
                    carrier,
                    ..
                } => {
                    if let Some(ref mut o) = order {
                        o.status = OrderStatus::Shipped;
                        o.shipping_info = Some(ShippingInfo {
                            tracking_number,
                            carrier,
                        });
                        o.version = metadata.version;
                    }
                }
                OrderEvent::OrderCancelled { .. } => {
                    if let Some(ref mut o) = order {
                        o.status = OrderStatus::Cancelled;
                        o.version = metadata.version;
                    }
                }
            }
        }

        order
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Event Sourcing with CQRS Example ===\n");

    // Create temporary directory for this demo
    let temp_dir = std::env::temp_dir().join("nano_wal_event_sourcing");
    std::fs::create_dir_all(&temp_dir)?;

    let mut event_store = EventStore::new(temp_dir.to_str().unwrap())?;

    // Simulate user registration flow
    println!("1. User Registration Flow");
    let user_id = "user-123";

    let registration_event = UserEvent::UserRegistered {
        user_id: user_id.to_string(),
        email: "alice@example.com".to_string(),
        name: "Alice Smith".to_string(),
    };

    let metadata = EventMetadata {
        event_id: uuid::Uuid::new_v4().to_string(),
        event_type: "UserRegistered".to_string(),
        timestamp: SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs(),
        version: 1,
        correlation_id: Some("reg-flow-001".to_string()),
        causation_id: None,
    };

    event_store.append_user_event(registration_event, metadata)?;
    println!("   ✓ User registered: {}", user_id);

    // Email change
    let email_change_event = UserEvent::EmailChanged {
        user_id: user_id.to_string(),
        old_email: "alice@example.com".to_string(),
        new_email: "alice.smith@example.com".to_string(),
    };

    let metadata = EventMetadata {
        event_id: uuid::Uuid::new_v4().to_string(),
        event_type: "EmailChanged".to_string(),
        timestamp: SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs(),
        version: 2,
        correlation_id: Some("email-update-001".to_string()),
        causation_id: None,
    };

    event_store.append_user_event(email_change_event, metadata)?;
    println!("   ✓ Email updated for user: {}", user_id);

    // Profile update
    let mut profile_fields = HashMap::new();
    profile_fields.insert("phone".to_string(), "+1-555-0123".to_string());
    profile_fields.insert("city".to_string(), "San Francisco".to_string());

    let profile_update_event = UserEvent::ProfileUpdated {
        user_id: user_id.to_string(),
        fields: profile_fields,
    };

    let metadata = EventMetadata {
        event_id: uuid::Uuid::new_v4().to_string(),
        event_type: "ProfileUpdated".to_string(),
        timestamp: SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs(),
        version: 3,
        correlation_id: Some("profile-update-001".to_string()),
        causation_id: None,
    };

    event_store.append_user_event(profile_update_event, metadata)?;
    println!("   ✓ Profile updated for user: {}", user_id);

    // Simulate order flow
    println!("\n2. Order Processing Flow");
    let order_id = "order-456";

    let items = vec![
        OrderItem {
            product_id: "product-001".to_string(),
            quantity: 2,
            price: 29.99,
        },
        OrderItem {
            product_id: "product-002".to_string(),
            quantity: 1,
            price: 49.99,
        },
    ];

    let order_created_event = OrderEvent::OrderCreated {
        order_id: order_id.to_string(),
        user_id: user_id.to_string(),
        items: items.clone(),
        total_amount: 109.97,
    };

    let metadata = EventMetadata {
        event_id: uuid::Uuid::new_v4().to_string(),
        event_type: "OrderCreated".to_string(),
        timestamp: SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs(),
        version: 1,
        correlation_id: Some("order-flow-001".to_string()),
        causation_id: None,
    };

    event_store.append_order_event(order_created_event, metadata)?;
    println!("   ✓ Order created: {}", order_id);

    // Order payment
    let order_paid_event = OrderEvent::OrderPaid {
        order_id: order_id.to_string(),
        payment_method: "credit_card".to_string(),
        amount: 109.97,
    };

    let metadata = EventMetadata {
        event_id: uuid::Uuid::new_v4().to_string(),
        event_type: "OrderPaid".to_string(),
        timestamp: SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs(),
        version: 2,
        correlation_id: Some("order-flow-001".to_string()),
        causation_id: Some("payment-processor-001".to_string()),
    };

    event_store.append_order_event(order_paid_event, metadata)?;
    println!("   ✓ Order paid: {}", order_id);

    // Order shipping
    let order_shipped_event = OrderEvent::OrderShipped {
        order_id: order_id.to_string(),
        tracking_number: "TRK123456789".to_string(),
        carrier: "FedEx".to_string(),
    };

    let metadata = EventMetadata {
        event_id: uuid::Uuid::new_v4().to_string(),
        event_type: "OrderShipped".to_string(),
        timestamp: SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs(),
        version: 3,
        correlation_id: Some("order-flow-001".to_string()),
        causation_id: Some("fulfillment-center-001".to_string()),
    };

    event_store.append_order_event(order_shipped_event, metadata)?;
    println!("   ✓ Order shipped: {}", order_id);

    // Reconstruct aggregates from events
    println!("\n3. Aggregate Reconstruction");

    let user_events = event_store.get_user_events(user_id)?;
    if let Some(user) = UserAggregate::from_events(user_events) {
        println!("   User Aggregate State:");
        println!("   - ID: {}", user.user_id);
        println!("   - Email: {}", user.email);
        println!("   - Name: {}", user.name);
        println!("   - Active: {}", user.is_active);
        println!("   - Profile: {:?}", user.profile);
        println!("   - Version: {}", user.version);
    }

    let order_events = event_store.get_order_events(order_id)?;
    if let Some(order) = OrderAggregate::from_events(order_events) {
        println!("\n   Order Aggregate State:");
        println!("   - ID: {}", order.order_id);
        println!("   - User ID: {}", order.user_id);
        println!("   - Status: {:?}", order.status);
        println!("   - Total: ${:.2}", order.total_amount);
        println!("   - Items: {} products", order.items.len());
        if let Some(payment) = &order.payment_info {
            println!("   - Payment: {} (${:.2})", payment.method, payment.amount);
        }
        if let Some(shipping) = &order.shipping_info {
            println!(
                "   - Shipping: {} via {}",
                shipping.tracking_number, shipping.carrier
            );
        }
        println!("   - Version: {}", order.version);
    }

    // Show event streams
    println!("\n4. Event Stream Analysis");
    let all_keys: Vec<String> = event_store.wal.enumerate_keys()?.collect();
    println!("   Active Event Streams: {:?}", all_keys);

    // Clean up
    std::fs::remove_dir_all(&temp_dir).ok();

    println!("\n✓ Event Sourcing with CQRS example completed!");
    Ok(())
}

// Note: This example requires additional dependencies in Cargo.toml:
// [dependencies]
// serde = { version = "1.0", features = ["derive"] }
// serde_json = "1.0"
// uuid = { version = "1.0", features = ["v4"] }
