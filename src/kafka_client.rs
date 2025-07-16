use rdkafka::{ClientConfig, producer::{BaseProducer, BaseRecord, Producer}};
use std::time::Duration;
use crate::md_structures::Bar1Min;
use std::sync::Arc;
use std::sync::LazyLock;
use rdkafka::error::KafkaError;
use once_cell::sync::Lazy;

/// 发送一条消息到 Kafka
pub fn send_message(producer: &BaseProducer, topic: &str, key: &str, message: &str) {
    match producer.send(BaseRecord::to(topic).payload(message).key(key)) {
        Ok(_) => println!("Message sent."),
        Err((e, _)) => eprintln!("Send error: {:?}", e),
    }

    // let _ = producer.flush(Duration::from_secs(1));
}

// 全局生产者
pub static KAFKA_PRODUCER: Lazy<Arc<BaseProducer>> = Lazy::new(|| {
    Arc::new(
        ClientConfig::new()
            .set("bootstrap.servers", "124.220.72.118:9092")
            .create()
            .expect("Failed to create Kafka producer"),
    )
});

pub fn send_bar_1min(bar: &Bar1Min) -> Result<(), KafkaError> {
    let producer = KAFKA_PRODUCER.clone();
    let topic = format!("bar_1min_{}", bar.contract.trim());
    let message = bar.build_json_message(); // 或 serde_json::to_string(bar)

    send_message(&producer, &topic, "", &message);
    println!(">>> 发送到 Kafka: topic={}, message={}", topic, message);
    Ok(())
}