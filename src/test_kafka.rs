use md_processor::md_structures::Bar1Min;
use md_processor::kafka_client::*;

use serde::{Serialize, Deserialize};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::config::ClientConfig;
use rdkafka::error::KafkaError;
use std::time::Duration;

fn main() {
    let topic = "test_topic";
    let key = "my_key";
    let message = "Hello from main!";

    kafka_client::send_message(topic, key, message);
}

// /// 示例 main
// #[tokio::main]
// async fn main() -> Result<(), KafkaError> {
//     let producer = create_kafka_producer("localhost:9092")?; // 换成你的broker地址

//     // 构造样例数据
//     let bar = Bar1Min {
//         contract: "cu2505".into(),
//         contract_yymm: "2505".into(),
//         comd: "cu".into(),
//         exchange: "SHFE".into(),
//         date: "2025-04-13".into(),
//         trade_time: "2025-04-13 09:30:00.123".into(),
//         open: 100.0, 
//         high: 101.0,
//         low: 99.5,
//         close: 100.7,
//         pre_settle: 98.8,
//         volume: 123,
//         turnover: 45678.9,
//         open_interest: 1000,
//         open_interest_diff: 25,
//         bid_price_1: 100.6,
//         bid_volume_1: 500,
//         ask_price_1: 100.8,
//         ask_volume_1: 400,
//         mid_price: 100.7,
//         vwap: 100.72,
//         maturity_month: 5,
//         maturity_day: 15,
//     };

//     send_bar_to_kafka(&producer, "bar_1min_topic", &bar).await?;
//     println!("Bar sent to Kafka.");
//     Ok(())
// }