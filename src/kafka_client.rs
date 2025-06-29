use rdkafka::{ClientConfig, producer::{BaseProducer, BaseRecord, Producer}};
use std::time::Duration;

/// 发送一条消息到 Kafka
pub fn send_message(producer: &BaseProducer, topic: &str, key: &str, message: &str) {
    match producer.send(BaseRecord::to(topic).payload(message).key(key)) {
        Ok(_) => println!("Message sent."),
        Err((e, _)) => eprintln!("Send error: {:?}", e),
    }

    let _ = producer.flush(Duration::from_secs(1));
}