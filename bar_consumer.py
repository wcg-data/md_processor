from confluent_kafka import Consumer, KafkaException, KafkaError, TopicPartition
from collections import defaultdict, deque
import json
import time

bootstrap_servers = '124.220.72.118:9092'

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

def consume_messages_lag_commit(topics, group_id):
    """
    :param topics: str 或 List[str]，Kafka 订阅的 topic 名称
    :param group_id: 消费组 ID
    """
    if isinstance(topics, str):
        topics = [topics]

    consumer_conf = {
        'bootstrap.servers': bootstrap_servers,
        'group.id': group_id,
        'auto.offset.reset': 'earliest',     # <<< 修改①：启动即读最新
        'enable.auto.commit': False        # <<< 修改②：关闭自动提交
    }

    consumer = Consumer(consumer_conf)
    consumer.subscribe(topics)

    print(f">>> 启动消费者，订阅主题: {topics}")

    # <<< 新增③：为每个分区维护一个固定长度 1001 的 deque
    LAG = 1000
    offset_cache = defaultdict(lambda: deque(maxlen=LAG + 1))

    BATCH_SIZE = 5000  
    try:
        while True:
            # msg = consumer.poll(timeout=1.0)
                # <<< 1. 一次拉回最多 5000 条，返回 list[Message]
            msgs = consumer.consume(num_messages=BATCH_SIZE, timeout=1.0)
            if not msgs:            # consume 可能返回 []
                continue

            for msg in msgs:        # <<< 2. 遍历批量里的每条 Message 继续原来的逻辑
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        print(f"%% {msg.topic()} [{msg.partition()}] reached end at offset {msg.offset()}")
                    else:
                        raise KafkaException(msg.error())
                else:
                    try:
                        bar_json = msg.value().decode('utf-8')
                        bar_data = json.loads(bar_json)
                        # print(json.dumps(bar_data, ensure_ascii=False, indent=2))
                        current_time = time.time()
                        milliseconds = int((current_time - int(current_time)) * 1000)
                        print(f"{bar_data.get('contract')}  {bar_data.get('trade_time')} {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}.{milliseconds:03d}")
                        # <<< 新增④：记录 offset，并在窗口满时提交最早那个
                        tp = TopicPartition(msg.topic(), msg.partition())
                        offset_cache[tp].append(msg.offset())

                        if len(offset_cache[tp]) >= LAG + 1:
                            commit_offset = offset_cache[tp][0] + 1
                            consumer.commit(
                                offsets=[TopicPartition(tp.topic, tp.partition, commit_offset)],
                                asynchronous=False
                            )
                            offset_cache[tp].popleft()          # <-- 关键：保持窗口恒为 LAG


                    except json.JSONDecodeError as e:
                        print(f"!!! JSON 解码失败: {e}，原始数据: {msg.value()}")
                    except Exception as ex:
                        print(f"!!! 处理消息时发生异常: {ex}")

    except KeyboardInterrupt:
        print(">>> 消费者被中断，正在退出...")
    finally:
        # <<< 新增⑤：进程退出前把各分区当前滞后 offset 提交一次
        for tp, dq in offset_cache.items():
            if len(dq) >= LAG + 1:          # <-- 条件改为 >= LAG+1
                consumer.commit(
                    offsets=[TopicPartition(tp.topic, tp.partition, dq[0] + 1)],
                    asynchronous=False
                )
        consumer.close()
        print(">>> 消费者已关闭")

freq = "1min"
# contract_list = ['al2605', 'sn2603']
# contract_list = ['PF512','CF603','sn2512','l2603','i2509','pb2512','rb2511','sn2606','SF601','pg2601','PF602','lu2605','v2507','v2605','CY604','nr2511','PM507','PX601','pb2603','i2605','UR507','si2512','sc2712','jd2512','IM2512','rr2507','rr2602','RM511','zn2602','si2507','fb2604','ag2602','a2603','CY511','v2510','a2507','TA605','bc2509','FG601','bz2603','c2603','bc2602','lu2509','al2511','cu2509','fu2605','lh2507','sc2806','PK603','wr2601','AP603','eb2604','l2602','bu2603','CY508','m2508','pg2605','ZC511','MA511','sp2510','wr2507','y2507','bb2603','rb2605','PX511','br2511','CJ509','jm2509','IH2507','ni2606','JR507','pg2511','p2509','PM601','b2605','nr2602','IM2508','j2603','T2509','RM603','SH605','SF510','pg2509','sc2511','jd2603','T2603','sp2604','PR606','b2602','ru2508','LR601','hc2606','cu2603','jm2603','br2601','UR510','SH511','j2606','fu2511','nr2604','zn2512','wr2605','al2602','TF2509','rb2601','CJ605','br2507','y2511','ZC602','TA507','ru2606','pb2509','PX507','ps2604','FG606','y2603','bc2510','ad2601','nr2601','al2606','rb2509','LR509','MA604','SM602','si2601','SH601','eg2603','hc2601','fb2605','SA604','UR604','lc2604','m2512','CY601','MA605','rr2510','SH602','PK511','SA507','SR601','cu2512','pg2606','TL2603','sc2607','b2507','ao2510','al2508','v2601','p2604','OI507','eg2507','rr2605','IF2512','sn2602','l2512','jd2507','lc2603','PR512','ad2605','bc2601','ss2509','lc2510','ec2602','zn2510','i2510','lu2512','IF2509','bc2606','ru2511','eg2512','pp2510','ps2508','SM605','au2606','ao2601','RI605','v2604','ru2601','lh2605','bz2606','OI509','pb2510','sp2603','fu2508','rr2601','fu2604','sp2507','ni2603','CJ603','ad2512','wr2602','rb2512','sc2606','eb2507','fb2508','SM512','fb2603','a2601','jm2507','sc2612','lu2604','CY603','FG511','RI603','ZC512','TA604','bc2605','PX602','bu2606','MA507','lh2511','UR603','PM605','i2604','sc2510','PR508','TS2603','CY602','eg2509','b2601','WH601','ZC605','AP512','l2604','RI509','ps2511','au2508','PR602','ao2507','v2511','j2604','au2510','ec2510','ec2604','MA512','ao2604','br2512','cs2603','lu2508','b2510','ps2603','PR605','IH2508','sn2508','pp2507','jm2606','fb2507','PF606','hc2509','br2508','eb2601','SM603','ag2603','bu2602','ZC601','ss2603','CY512','ec2508','sc2601','PK605','lc2605','pb2602','PX512','pp2604','m2507','pg2510','j2509','lg2507','PR510','hc2603','sn2605','si2508','zn2606','i2507','l2601','rb2507','IC2509','l2507','bu2609','lg2511','fu2601','sn2601','lg2605','RS509','br2604','FG604','SH603','ss2510','PR511','rb2508','jm2602','si2602','TF2603','MA603','cu2510','pp2601','ss2602','TA512','RM508','IM2507','eb2605','SF507','PM511','pb2606','p2603','CY507','sn2509','cs2509','ZC607','ad2606','CF509','ss2604','PF509','SR507','OI603','bu2605','sc2604','ao2605','j2605','j2510','SA510','nr2508','SH510','SH509','bb2605','PK604','al2603','au2602','b2606','SF604','cu2606','WH509','lh2601','ad2511','ru2605','sc2803','nr2512','al2507','eg2602','SH606','jd2604','pp2511','a2511','SA603','PK510','lc2509','ao2606','lc2511','lu2510','WH511','zn2511','ag2511','rb2602','wr2606','FG509','ss2507','jm2510','rr2606','sn2510','ps2512','TL2509','lc2602','SM606','au2509','WH605','RI511','PF604','ad2602','bu2512','TA601','l2511','PR507','cu2602','y2512','eg2606','ni2510','JR601','SM509','lc2512','eb2512','hc2511','ni2604','wr2510','SM511','bu2510','ss2512','PR601','UR511','TA603','eg2605','v2512','eg2510','bc2604','OI605','lc2606','SH507','UR602','sn2511','RM601','p2512','jd2601','SM507','ag2606','nr2510','ag2510','ru2510','jm2605','fu2607','lh2509','ZC508','PK601','eb2508','cs2605','sc2605','AP604','fb2510','fb2509','ps2509','zn2508','PX508','pg2603','v2603','v2509','PX603','lu2607','wr2512','bb2509','pb2605','IC2508','RS508','bu2706','i2603','ZC604','cs2511','lc2508','SF509','bu2508','sn2507','fu2512','SF605','JR511','zn2507','rb2604','TS2509','PF605','FG605','WH603','jd2510','sc2709','eg2601','j2601','sc2602','bc2507','ru2507','SF512','b2604','PR604','ao2603','bz2605','ag2509','al2509','CF601','fb2512','SA511','c2509','j2508','m2601','au2512','PR509','wr2603','al2604','zn2605','l2510','AP601','SA601','PK512','SF603','i2508','FG512','br2509','lu2603','p2602','JR605','ao2508','PM509','bu2601','pp2509','l2508','bu2703','b2511','y2509','ps2602','pb2511','pp2603','cu2508','IH2509','CJ507','SR605','bu2604','sc2603','si2603','bu2509','ag2512','ss2601','MA509','SR509','m2605','sp2512','SF508','bu2507','PF508','a2509','bu2612','FG603','pg2507','au2604','nr2507','ec2606','ad2603','sc2703','PF603','CF605','l2605','hc2604','TL2512','nr2606','b2509','rr2511','eb2602','cu2605','FG508','si2511','br2603','ao2512','p2606','y2605','CF507','UR509','lc2601','jd2509','bc2512','sp2508','UR601','si2606','pp2602','MA602','JR509','PX606','IF2507','si2509','TF2512','fb2602','UR606','SM508','br2605','TA511','zn2601','RM605','lg2509','pp2512','SF606','ss2605','sp2602','T2512','jd2605','a2605','pb2508','AP511','jm2601','SR603','ni2601','j2511','cu2511','IH2512','hc2512','rr2509','sc2609','ps2507','TA509','jm2508','al2512','rr2603','fu2602','ru2604','SM510','lg2601','PX509','AP605','ag2601','zn2509','lu2606','PX604','UR605','i2602','jm2511','pp2606','p2510','jd2511','ps2510','j2507','lg2603','IC2512','hc2510','SA602','TS2512','zn2604','SF602','ao2509','CF511','hc2602','CJ512','SH508','rr2508','RI507','bu2511','ni2508','SH604','ni2512','rb2510','jd2602','sp2601','bz2604','ag2605','ao2602','ag2508','i2606','PF511','ni2511','ni2605','sc2509','fu2606','ZC606','ps2606','eb2509','wr2511','SA508','PF601','pp2508','ZC510','CY605','nr2509','lu2602','sc2512','m2509','ZC603','b2603','ZC509','fb2511','lc2507','cu2604','OI601','al2605','c2511','ss2511','SA606','RI601','ec2512','eg2511','v2602','ru2603','pg2604','bc2508','j2602','PF510','SF511','p2511','l2509','v2508','lh2603','UR512','b2512','y2508','rb2606','eb2606','ps2601','RS511','PR603','sp2605','cu2507','hc2507','al2510','c2601','jm2604','RM509','eb2511','cs2601','i2512','CJ601','rb2603','sc2706','p2508','pg2602','LR511','ao2511','p2601','nr2603','bc2603','TA606','CY509','bb2601','fb2606','c2605','wr2508','eg2508','p2507','MA606','pg2508','PM603','cs2507','SM604','RM507','wr2509','FG510','OI511','hc2605','l2606','SA512','ni2602','pg2512','SH512','jm2512','v2606','cu2601','JR603','ru2509','al2601','rr2512','b2508','TA508','hc2508','IM2509','sp2606','MA510','m2511','sn2603','IC2507','ps2605','bb2511','UR508','sn2604','SA605','ag2604','jd2508','br2510','pb2507','si2604','si2510','sp2509','br2602','fu2510','MA508','eb2510','SM601','eb2603','CY510','rr2604','pb2601','c2507','p2605','zn2603','PX510','pb2604','PF507','au2507','ss2606','bc2511','TA602','ad2604','ss2508','AP510','j2512','fu2509','si2605','lu2601','ni2509','LR507','i2511','ag2507','pp2605','SA509','m2603','WH507','fu2603','sc2508','eg2604','br2606','TA510','sp2511','SR511','FG602','jd2606','RS507','i2601','CY606','ni2507','MA601','IF2508','nr2605','PX605','wr2604','FG507','y2601','fb2601','lu2511']
contract_list = ['sn2606']

topic = [f"bar_{freq}_{contract}" for contract in contract_list]
# print(topic)
group_id = "bar-consumer-6"


consume_messages_lag_commit(topic, group_id)
# consume_messages(topic, group_id)
