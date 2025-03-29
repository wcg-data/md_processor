//! 实时行情数据处理程序
//! 
//! 该程序从MySQL数据库获取商品列表，订阅对应的Kafka主题，
//! 解析接收到的CSV格式行情数据，并在多线程环境下处理这些数据。

use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::Message;
use serde::Deserialize;
use sqlx::{mysql::MySqlPoolOptions, Row};
use std::time::Duration;
use std::thread;
use std::sync::{Arc, Mutex};
use std::collections::HashMap;
use chrono::{NaiveDateTime, Timelike, NaiveTime, NaiveDate, Duration as ChronoDuration};

// ==================== 数据结构定义 ====================

/// 行情数据结构体
/// 
/// 表示从Kafka接收到并解析的单条行情数据
#[derive(Debug, Deserialize, Clone)]
struct Tick {
    trade_time: String,    // 交易时间
    contract: String,      // 合约代码
    opencol: f64,          // 开盘价
    high: f64,             // 最高价
    low: f64,              // 最低价
    close: f64,            // 收盘价/最新价
    volume: u64,           // 成交量
    open_interest: u64,    // 持仓量
    turnover: f64,         // 成交额
    pre_settle: f64,       // 昨结算价
    buy_price_1: f64,      // 买一价
    ask_price_1: f64,      // 卖一价
    bid_volume_1: u32,     // 买一量
    ask_volume_1: u32,     // 卖一量
    mid_price: f64,        // 中间价
    comd: String,          // 品种代码
    exchange: String,      // 交易所代码
}

/// 1分钟K线数据结构体
#[derive(Debug, Clone)]
struct Bar1Min {
    trade_time: NaiveDateTime,  // 交易时间（分钟级别）
    contract: String,           // 合约代码
    opencol: f64,               // 开盘价
    high: f64,                  // 最高价
    low: f64,                   // 最低价
    close: f64,                 // 收盘价
    volume: u64,                // 成交量
    open_interest: u64,         // 持仓量
    turnover: f64,              // 成交额
    pre_settle: f64,            // 昨结算价
    open_interest_diff: i64,    // 持仓量变化 - 修改为i64类型
    buy_price_1: f64,           // 买一价
    ask_price_1: f64,           // 卖一价
    bid_volume_1: u32,          // 买一量
    ask_volume_1: u32,          // 卖一量
    mid_price: f64,             // 中间价
    merge_count: u32,           // 合并的Tick数量
    vwap: f64,                  // 成交量加权平均价
    comd: String,               // 品种代码
    exchange: String,           // 交易所代码
}

/// Tick聚合数据结构体，用于计算1分钟K线
struct BarAggregator {
    ticks: Vec<Tick>,              // 当前分钟内所有Tick数据
    prev_volume: u64,              // 上一个Tick的成交量，用于计算增量
    volume_price_sum: f64,         // 成交量*价格的累计，用于计算VWAP
}

impl BarAggregator {
    /// 创建新的聚合器
    fn new() -> Self {
        Self {
            ticks: Vec::new(),
            prev_volume: 0,
            volume_price_sum: 0.0,
        }
    }

    /// 添加Tick数据并更新聚合状态
    fn add_tick(&mut self, tick: Tick) {
        let volume_delta = if self.ticks.is_empty() {
            tick.volume
        } else {
            tick.volume.saturating_sub(self.prev_volume)
        };
        
        self.volume_price_sum += volume_delta as f64 * tick.close;
        self.prev_volume = tick.volume;
        self.ticks.push(tick);
    }

    /// 生成1分钟K线数据
    fn generate_bar(&self, minute_time: NaiveDateTime) -> Option<Bar1Min> {
        if self.ticks.is_empty() {
            return None;
        }

        let first_tick = &self.ticks[0];
        let last_tick = &self.ticks[self.ticks.len() - 1];
        
        // 计算最高价和最低价
        let high = self.ticks.iter().map(|t| t.high).fold(f64::NEG_INFINITY, f64::max);
        let low = self.ticks.iter().map(|t| t.low).fold(f64::INFINITY, f64::min);
        
        // 计算总成交量
        let total_volume = last_tick.volume;
        
        // 计算VWAP
        let vwap = if total_volume > 0 {
            self.volume_price_sum / total_volume as f64
        } else {
            last_tick.close
        };

        // 创建并返回Bar对象
        Some(Bar1Min {
            trade_time: minute_time,
            contract: first_tick.contract.clone(),
            opencol: first_tick.close,
            high,
            low,
            close: last_tick.close,
            volume: total_volume,
            open_interest: last_tick.open_interest,
            turnover: last_tick.turnover,
            pre_settle: last_tick.pre_settle,
            open_interest_diff: last_tick.open_interest as i64 - first_tick.open_interest as i64,
            buy_price_1: last_tick.buy_price_1,
            ask_price_1: last_tick.ask_price_1,
            bid_volume_1: last_tick.bid_volume_1,
            ask_volume_1: last_tick.ask_volume_1,
            mid_price: if last_tick.mid_price != 0.0 { last_tick.mid_price } else { last_tick.close },
            merge_count: self.ticks.len() as u32,
            vwap,
            comd: first_tick.comd.clone(),
            exchange: first_tick.exchange.clone(),
        })
    }
}

// ==================== 配置常量 ====================

/// Kafka服务器地址
const KAFKA_BOOTSTRAP_SERVERS: &str = "localhost:9092";
/// Kafka消费组ID
const KAFKA_GROUP_ID: &str = "bar-consumer-4";
/// 数据库连接URL
const DB_URL: &str = "mysql://root:gXi{<ZQ2&N9ipjTtg;bE@main.mcga.work:32089/future_1d";
/// 工作线程数上限
const MAX_WORKER_THREADS: usize = 12;
/// 主线程轮询间隔(毫秒)
const POLL_INTERVAL_MS: u64 = 100;
/// 工作线程空闲等待时间(毫秒)
const WORKER_IDLE_WAIT_MS: u64 = 1;

// ==================== 数据解析函数 ====================

/// 解析CSV格式的行情数据
///
/// # 参数
/// * `csv_str` - CSV格式的行情数据字符串
///
/// # 返回
/// * `Result<Tick, String>` - 解析成功返回Tick结构体，失败返回错误信息
fn parse_tick_from_csv(csv_str: &str) -> Result<Tick, String> {
    // 按逗号分割CSV字符串
    let fields: Vec<&str> = csv_str.split(',').collect();
    
    // 验证字段数量是否正确
    if fields.len() != 17 {
        return Err(format!("CSV格式不正确，预期17个字段，但得到 {}", fields.len()));
    }
    
    // 尝试解析各个字段
    let opencol = fields[2].parse::<f64>().map_err(|e| format!("解析opencol失败: {}", e))?;
    let high = fields[3].parse::<f64>().map_err(|e| format!("解析high失败: {}", e))?;
    let low = fields[4].parse::<f64>().map_err(|e| format!("解析low失败: {}", e))?;
    let close = fields[5].parse::<f64>().map_err(|e| format!("解析close失败: {}", e))?;
    let volume = fields[6].parse::<u64>().map_err(|e| format!("解析volume失败: {}", e))?;
    let open_interest = fields[7].parse::<f64>().map_err(|e| format!("解析open_interest失败: {}", e))?.round() as u64;
    let turnover = fields[8].parse::<f64>().map_err(|e| format!("解析turnover失败: {}", e))?;
    let pre_settle = fields[9].parse::<f64>().map_err(|e| format!("解析pre_settle失败: {}", e))?;
    let buy_price_1 = fields[10].parse::<f64>().map_err(|e| format!("解析buy_price_1失败: {}", e))?;
    let ask_price_1 = fields[11].parse::<f64>().map_err(|e| format!("解析ask_price_1失败: {}", e))?;
    let bid_volume_1 = fields[12].parse::<u32>().map_err(|e| format!("解析bid_volume_1失败: {}", e))?;
    let ask_volume_1 = fields[13].parse::<u32>().map_err(|e| format!("解析ask_volume_1失败: {}", e))?;
    let mid_price = fields[14].parse::<f64>().map_err(|e| format!("解析mid_price失败: {}", e))?;
    
    // 构建并返回Tick结构体
    Ok(Tick {
        trade_time: fields[0].to_string(),
        contract: fields[1].to_string(),
        opencol,
        high,
        low,
        close,
        volume,
        open_interest,
        turnover,
        pre_settle,
        buy_price_1,
        ask_price_1,
        bid_volume_1,
        ask_volume_1,
        mid_price,
        comd: fields[15].to_string(),
        exchange: fields[16].to_string(),
    })
}

/// 解析时间字符串为NaiveDateTime
fn parse_datetime(time_str: &str) -> Result<NaiveDateTime, String> {
    NaiveDateTime::parse_from_str(time_str, "%Y-%m-%d %H:%M:%S%.f")
        .map_err(|e| format!("解析时间失败: {}", e))
}

/// 计算分钟时间戳，根据特定的规则处理时间
fn calculate_minute_time(dt: NaiveDateTime) -> NaiveDateTime {
    let time = dt.time();
    let date = dt.date();
    
    // 处理特殊时间区间
    if time >= NaiveTime::from_hms_opt(8, 55, 0).unwrap() && 
       time <= NaiveTime::from_hms_opt(9, 0, 0).unwrap() {
        // [08:55:00, 09:00:00] 区间
        date.and_hms_opt(9, 0, 0).unwrap()
    } else if time >= NaiveTime::from_hms_opt(20, 55, 0).unwrap() && 
              time <= NaiveTime::from_hms_opt(21, 0, 0).unwrap() {
        // [20:55:00, 21:00:00] 区间
        date.and_hms_opt(21, 0, 0).unwrap()
    } else {
        // 其他按照左开右闭
        if time.second() == 0 && time.nanosecond() == 0 {
            // 如果秒和微秒都是0，即整分钟
            NaiveDateTime::new(date, 
                NaiveTime::from_hms_opt(time.hour(), time.minute(), 0).unwrap())
        } else {
            // 否则取下一分钟
            let truncated = NaiveDateTime::new(date, 
                NaiveTime::from_hms_opt(time.hour(), time.minute(), 0).unwrap());
            truncated + ChronoDuration::minutes(1)
        }
    }
}

// ==================== 业务处理函数 ====================

/// 处理单条Tick数据并更新1分钟K线聚合器
///
/// # 参数
/// * `tick` - 行情数据结构体
/// * `topic` - 数据来源主题
/// * `bar_aggregators` - 分钟K线聚合器映射表
/// * `completed_bars` - 已完成的1分钟K线数据容器
fn process_tick_and_update_bars(
    tick: Tick, 
    topic: &str,
    bar_aggregators: &mut HashMap<String, HashMap<NaiveDateTime, BarAggregator>>,
    completed_bars: &mut Vec<Bar1Min>
) -> Result<(), String> {
    // 解析交易时间
    let dt = parse_datetime(&tick.trade_time)?;
    
    // 计算对应的分钟时间
    let minute_time = calculate_minute_time(dt);
    
    // 获取或创建该合约的聚合器映射
    let contract_aggregators = bar_aggregators
        .entry(tick.contract.clone())
        .or_insert_with(HashMap::new);
    
    // 获取或创建该分钟时间的聚合器
    let aggregator = contract_aggregators
        .entry(minute_time)
        .or_insert_with(BarAggregator::new);
    
    // 添加Tick数据到聚合器
    aggregator.add_tick(tick.clone());
    
    // 检查是否需要生成并输出完成的K线
    // 例如：当前时间大于某个聚合器的分钟时间+1分钟，说明该K线已经完成
    let current_time = parse_datetime(&tick.trade_time)?;
    
    // 找出所有可能已完成的K线（当前时间已超过该分钟+1分钟）
    let mut completed_minutes = Vec::new();
    
    for (&agg_minute, _) in contract_aggregators.iter() {
        // 如果当前分钟与聚合分钟不同，且当前时间已超过聚合分钟至少1分钟
        if agg_minute != minute_time && current_time >= agg_minute + ChronoDuration::minutes(1) {
            completed_minutes.push(agg_minute);
        }
    }
    
    // 处理所有已完成的K线
    for completed_minute in completed_minutes {
        if let Some(aggregator) = contract_aggregators.remove(&completed_minute) {
            if let Some(bar) = aggregator.generate_bar(completed_minute) {
                // 处理生成的1分钟K线数据
                println!("生成1分钟K线: {} - {}", bar.contract, bar.trade_time);
                completed_bars.push(bar);
            }
        }
    }
    
    Ok(())
}

// ==================== 数据库操作函数 ====================

/// 从数据库获取商品代码列表
///
/// # 返回
/// * 包含所有商品代码的字符串向量
fn fetch_commodity_codes() -> Result<Vec<String>, Box<dyn std::error::Error>> {
    // 创建一个简单的运行时来执行一次性的数据库查询
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    
    // 在运行时内执行异步数据库操作
    runtime.block_on(async {
        // 创建数据库连接池
        let pool = MySqlPoolOptions::new()
            .max_connections(1) // 单一连接足够查询操作
            .connect(DB_URL)
            .await?;
        
        println!("数据库连接成功，开始获取商品代码...");
        
        // 执行SQL查询获取商品代码
        let rows = sqlx::query("SELECT DISTINCT comd FROM future_1d.main_daily WHERE method = 'turnover'")
            .fetch_all(&pool)
            .await?;
        
        // 从查询结果中提取商品代码
        let codes: Vec<String> = rows.iter()
            .map(|row| row.get::<String, _>(0))
            .collect();
        
        // 查询完成后关闭连接池
        pool.close().await;
        
        Ok::<_, sqlx::Error>(codes)
    })
    .map_err(|e| e.into())
}

// ==================== Kafka相关函数 ====================

/// 创建并配置Kafka消费者
///
/// # 返回
/// * 配置好的BaseConsumer实例
fn create_kafka_consumer() -> Result<BaseConsumer, rdkafka::error::KafkaError> {
    ClientConfig::new()
        .set("bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .set("group.id", KAFKA_GROUP_ID)
        .set("auto.offset.reset", "earliest")
        .create()
}

/// 启动工作线程处理消息
///
/// # 参数
/// * `messages` - 共享的消息队列
/// * `commodity_codes` - 商品代码列表
/// * `worker_count` - 工作线程数量
///
/// # 返回
/// * 工作线程句柄列表
fn start_worker_threads(
    messages: Arc<Mutex<HashMap<String, Vec<Tick>>>>,
    commodity_codes: Vec<String>,
    worker_count: usize
) -> Vec<thread::JoinHandle<()>> {
    let mut handles = vec![];
    
    // 创建指定数量的工作线程
    for worker_id in 0..worker_count {
        let worker_messages = Arc::clone(&messages);
        let worker_codes = commodity_codes.clone();
        
        // 启动工作线程
        let handle = thread::spawn(move || {
            println!("工作线程 {} 启动", worker_id);
            
            // 创建各合约的分钟K线聚合器映射
            let mut bar_aggregators: HashMap<String, HashMap<NaiveDateTime, BarAggregator>> = HashMap::new();
            // 存储已完成的1分钟K线
            let mut completed_bars: Vec<Bar1Min> = Vec::new();
            
            // 工作线程主循环
            loop {
                let mut batch_processed = false;
                
                // 尝试获取并处理消息
                if let Ok(mut msg_map) = worker_messages.lock() {
                    // 遍历所有商品代码查找待处理消息
                    for topic in &worker_codes {
                        if let Some(msg_vec) = msg_map.get_mut(topic) {
                            if !msg_vec.is_empty() {
                                // 获取并移除最后一条消息(栈结构)
                                if let Some(tick) = msg_vec.pop() {
                                    // 释放锁后处理消息，减少锁持有时间
                                    drop(msg_map);
                                    
                                    // 处理Tick数据并更新1分钟K线聚合器
                                    match process_tick_and_update_bars(tick, topic, &mut bar_aggregators, &mut completed_bars) {
                                        Ok(_) => {},
                                        Err(e) => eprintln!("处理Tick数据失败: {}", e),
                                    }
                                    
                                   batch_processed = true;
                                    break;
                                }
                            }
                        }
                    }
                }
                
                // 处理已完成的K线数据
                while let Some(bar) = completed_bars.pop() {
                    // // 在这里处理1分钟K线数据，例如写入数据库、发送到另一个Kafka主题等
                    // println!("处理1分钟K线: {} - {} OHLC: [{:.2}, {:.2}, {:.2}, {:.2}] 成交量: {}",
                    //          bar.contract, bar.trade_time, 
                    //          bar.opencol, bar.high, bar.low, bar.close, bar.volume);
                    //打印完整的bar_1min
                    println!("完整的bar_1min: {:?}", bar);
                }
                
                // 没有消息时短暂休眠，避免CPU空转
                if !batch_processed {
                    thread::sleep(Duration::from_millis(WORKER_IDLE_WAIT_MS));
                }
            }
        });
        
        handles.push(handle);
    }
    
    handles
}

/// 消费Kafka消息并分发到工作队列
///
/// # 参数
/// * `consumer` - Kafka消费者
/// * `messages` - 共享消息队列
fn consume_and_dispatch_messages(
    consumer: &BaseConsumer,
    messages: Arc<Mutex<HashMap<String, Vec<Tick>>>>
) -> ! {
    // 无限循环消费消息
    loop {
        // 轮询Kafka获取消息，超时时间设为100ms
        match consumer.poll(Duration::from_millis(POLL_INTERVAL_MS)) {
            Some(message_result) => {
                match message_result {
                    Ok(message) => {
                        let topic = message.topic();
                        
                        // 尝试解析消息负载
                        if let Some(Ok(csv_str)) = message.payload_view::<str>() {
                            // 解析CSV数据为Tick结构
                            match parse_tick_from_csv(csv_str) {
                                Ok(tick) => {
                                    // 将消息添加到对应主题的队列
                                    if let Ok(mut msg_map) = messages.lock() {
                                        msg_map.entry(topic.to_string())
                                            .or_insert_with(Vec::new)
                                            .push(tick);
                                    }
                                },
                                Err(e) => eprintln!("主题[{}]的CSV解析错误: {}", topic, e),
                            }
                        }
                    },
                    Err(e) => eprintln!("Kafka消息错误: {}", e),
                }
            },
            None => {
                // 没有消息时短暂休眠
                thread::sleep(Duration::from_millis(10));
            }
        }
    }
}

// ==================== 主函数 ====================

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("==== 实时行情数据处理系统启动 ====");
    
    // 1. 从数据库获取商品代码列表
    let commodity_codes = fetch_commodity_codes()?;
    println!("获取到{}个商品代码: {:?}", commodity_codes.len(), commodity_codes);
    
    // 检查是否获取到商品代码
    if commodity_codes.is_empty() {
        eprintln!("未找到任何商品代码，程序退出");
        return Ok(());
    }
    
    // 2. 确定工作线程数量(不超过系统限制)
    let worker_count = std::cmp::min(MAX_WORKER_THREADS, commodity_codes.len());
    println!("创建{}个工作线程来处理数据", worker_count);
    
    // 3. 创建Kafka消费者
    let consumer = create_kafka_consumer()?;

    // 4. 订阅Kafka主题
    let topics: Vec<&str> = commodity_codes.iter().map(AsRef::as_ref).collect();
    println!("正在订阅以下主题: {:?}", topics);
    consumer.subscribe(&topics)?;
    println!("Kafka消费者准备就绪，开始接收消息...");

    // 5. 创建线程安全的消息队列
    let messages = Arc::new(Mutex::new(HashMap::<String, Vec<Tick>>::new()));
    
    // 6. 启动工作线程
    let _handles = start_worker_threads(Arc::clone(&messages), commodity_codes, worker_count);
    
    // 7. 主线程开始消费消息并分发到队列
    consume_and_dispatch_messages(&consumer, messages);
    
    // 注意：这里永远不会执行到，因为上面的函数永远不会返回
    #[allow(unreachable_code)]
    Ok(())
}

