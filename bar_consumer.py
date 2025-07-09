from confluent_kafka import Consumer, KafkaException, KafkaError
import json

bootstrap_servers = '124.220.72.118:9092'

def on_bar_old(bar):
    """
    实时处理一条bar数据
    :param bar: dict 格式，包含K线的时间、价格、成交量等字段
    """
    print(f"[on_bar] 接收到 bar 数据: {bar}")
    # 示例处理：比如更新数据库、更新策略状态、写入文件等
    # 你可以根据需要自定义逻辑
    # process_bar(bar)
    pass

def on_bar(bar: dict):
    print(f"[{bar['trade_time']}] {bar['contract']}: open={bar['open']}, close={bar['close']}")

def consume_messages(topics, group_id):
    """
    :param topics: str 或 List[str]，Kafka 订阅的 topic 名称
    :param group_id: 消费组 ID
    :param on_bar: 回调函数，接受解析后的 bar dict
    """
    if isinstance(topics, str):
        topics = [topics]

    consumer_conf = {
        'bootstrap.servers': bootstrap_servers,
        'group.id': group_id,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': True
    }

    consumer = Consumer(consumer_conf)
    consumer.subscribe(topics)

    print(f">>> 启动消费者，订阅主题: {topics}")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print(f"%% {msg.topic()} [{msg.partition()}] reached end at offset {msg.offset()}")
                else:
                    raise KafkaException(msg.error())
            else:
                try:
                    # 解码 + 解析 JSON
                    bar_json = msg.value().decode('utf-8')
                    bar_data = json.loads(bar_json)
		   # 直接打印完整 JSON 对象
                    print(json.dumps(bar_data, ensure_ascii=False, indent=2))

                except json.JSONDecodeError as e:
                    print(f"!!! JSON 解码失败: {e}，原始数据: {msg.value()}")
                except Exception as ex:
                    print(f"!!! 处理消息时发生异常: {ex}")

    except KeyboardInterrupt:
        print(">>> 消费者被中断，正在退出...")
    finally:
        consumer.close()
        print(">>> 消费者已关闭")

def consume_messages_old(topic, group_id):
    # 配置Kafka消费者
    consumer_conf = { 
        'bootstrap.servers': bootstrap_servers,
        'group.id': group_id,
        'auto.offset.reset': 'earliest'
    }
    consumer = Consumer(**consumer_conf)
    consumer.subscribe(topic)

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print('%% %s [%d] reached end at offset %d\n' % (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                bar = msg.value().decode('utf-8')
                # print('Received message: {}'.format(msg.value().decode('utf-8')))
                on_bar(bar)
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

freq = "1min"
#contract_list = ['ag2603', 'c2603']
contract_list = ['al2605']

topic = [f"bar_{freq}_{contract}" for contract in contract_list]
# print(topic)
group_id = "bar-consumer-2"

consume_messages(topic, group_id)
