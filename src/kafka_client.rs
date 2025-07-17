use rdkafka::{ClientConfig, producer::{BaseProducer, BaseRecord}};
// use rdkafka::error::KafkaError;
use crate::md_structures::Bar1Min;
use std::sync::Arc;
use once_cell::sync::Lazy;


// 全局生产者
pub static KAFKA_PRODUCER: Lazy<Arc<BaseProducer>> = Lazy::new(|| {
    Arc::new(
        ClientConfig::new()
            .set("bootstrap.servers", "124.220.72.118:9092")
            .create()
            .expect("Failed to create Kafka producer"),
    )
});

/// 发送一条消息到 Kafka
pub fn send_message(producer: &BaseProducer, topic: &str, key: &str, message: &str) {
    match producer.send(BaseRecord::to(topic).payload(message).key(key)) {
        Ok(_) => println!("Message sent to Kafka: topic={}, message={}", topic, message),
        Err((e, _)) => eprintln!("Send error: {:?}", e),
    }

    // let _ = producer.flush(Duration::from_secs(1));
}

pub fn send_bar_1min(bar: &Bar1Min) {
    let producer = KAFKA_PRODUCER.clone();
    let topic = format!("bar_1min_{}", bar.contract.trim());
    let message = bar.build_json_message(); // 或 serde_json::to_string(bar)
    // let message = self.build_csv_message();

    send_message(&producer, &topic, "", &message);
}
