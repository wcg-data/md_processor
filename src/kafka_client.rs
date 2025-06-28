use rdkafka::{ClientConfig, producer::{BaseProducer, BaseRecord, Producer}};
use std::time::Duration;

/// 发送一条消息到 Kafka
pub fn send_message(topic: &str, key: &str, message: &str) {
    let producer: BaseProducer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .create()
        .expect("Failed to create Kafka producer");

    producer.send(
        BaseRecord::to(topic)
            .payload(message)
            .key(key),
    ).expect("Failed to send message");

    producer.flush(Duration::from_secs(1));
}