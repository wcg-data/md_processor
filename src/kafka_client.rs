use rdkafka::{ClientConfig, producer::{BaseProducer, BaseRecord, Producer}};
use std::time::Duration;
use crate::md_structures::Bar1Min;
use std::sync::Arc;
use std::sync::LazyLock;
use rdkafka::error::KafkaError;
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
        Ok(_) => println!("Message sent."),
        Err((e, _)) => eprintln!("Send error: {:?}", e),
    }

    // let _ = producer.flush(Duration::from_secs(1));
}

pub fn send_bar_1min(bar: &Bar1Min) -> Result<(), KafkaError> {
    let producer = KAFKA_PRODUCER.clone();
    let topic = format!("bar_1min_{}", bar.contract.trim());
    let message = bar.build_json_message(); // 或 serde_json::to_string(bar)

    send_message(&producer, &topic, "", &message);
    println!(">>> 发送到 Kafka: topic={}, message={}", topic, message);
    Ok(())
}


// // 向 Kafka 发送合约行情数据（JSON 格式）
// pub fn send_to_kafka(&self) {
//     let producer = KAFKA_PRODUCER.clone();
//     let topic = format!("bar_1min_{}", self.contract.trim());
//     let message = self.build_json_message(); // 生成指定字段的 JSON

//     kafka_client::send_message(&producer, &topic, "", &message);
//     println!(">>> 发送到 Kafka: topic={}, message={}", topic, message);
// }

// 向 Kafka 发送合约行情数据（CSV 格式）
// pub fn send_to_kafka(&self) {
//     let producer = KAFKA_PRODUCER.clone(); // Arc clone，零开销
//     let topic = format!("bar_1min_{}", self.contract.trim());
//     let message = self.build_csv_message();

//     kafka_client::send_message(&producer, &topic, "", &message);
//     println!(">>> 发送到 Kafka: topic={}", topic);
// }
