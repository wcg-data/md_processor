from confluent_kafka import Consumer, KafkaException, KafkaError
import json

bootstrap_servers = '124.220.72.118:9092'

def on_bar(bar):
    """
    实时处理一条bar数据
    :param bar: dict 格式，包含K线的时间、价格、成交量等字段
    """
    print(f"[on_bar] 接收到 bar 数据: {bar}")
    # 示例处理：比如更新数据库、更新策略状态、写入文件等
    # 你可以根据需要自定义逻辑
    # process_bar(bar)
    pass


def consume_messages(topic, group_id):
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
contract_list = ['ag2603', 'c2603']

topic = [f"bar_{freq}_{contract}" for contract in contract_list]
# print(topic)
group_id = "bar-consumer-2"

consume_messages(topic, group_id)