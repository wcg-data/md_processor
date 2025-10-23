# bar_to_csv_exporter.py - 从Kafka导出1min bar数据到CSV文件
# 功能：订阅所有bar_1min_*主题，将JSON数据转换为md_snapshot格式CSV
# 输出格式：contract,product,exchange,date,trade_time,open,high,low,bid,ask,bid_volume,turnover,volume,prev_close,ask_volume,open_interest,open_interest_diff,close

from confluent_kafka import Consumer, KafkaException, KafkaError
from datetime import datetime
import json
import csv
import os

bootstrap_servers = '124.220.72.118:9092'

def extract_product(contract: str) -> str:
    """从合约代码提取品种代码"""
    # 移除数字，保留字母部分
    product = ''.join([c for c in contract if c.isalpha()])
    return product.upper()

def bar_to_csv_row(bar_data: dict) -> list:
    """
    将bar JSON数据转换为CSV行

    CSV格式（18列）：
    contract,product,exchange,date,trade_time,open,high,low,bid,ask,
    bid_volume,turnover,volume,prev_close,ask_volume,open_interest,open_interest_diff,close
    """
    contract = bar_data.get('contract', '').strip()
    product = extract_product(contract)
    exchange = bar_data.get('exchange', '').strip()

    # 解析 trade_time，提取日期和时间
    trade_time_str = bar_data.get('trade_time', '')
    try:
        # 假设格式如 "2025-10-17 19:15:00"
        dt = datetime.strptime(trade_time_str, '%Y-%m-%d %H:%M:%S')
        date = dt.strftime('%Y-%m-%d')
        trade_time = trade_time_str + '.000'  # 添加毫秒
    except:
        date = ''
        trade_time = trade_time_str

    # 映射字段，使用空字符串表示缺失值
    def get_value(key, default=''):
        val = bar_data.get(key)
        if val is None or val == '':
            return default
        # 处理浮点数为0的情况
        if isinstance(val, (int, float)) and val == 0 and key in ['open', 'high', 'low']:
            return ''
        return val

    row = [
        contract,                              # contract
        product,                               # product
        exchange,                              # exchange
        date,                                  # date
        trade_time,                           # trade_time
        get_value('open'),                    # open
        get_value('high'),                    # high
        get_value('low'),                     # low
        get_value('bid_price_1'),             # bid
        get_value('ask_price_1'),             # ask
        get_value('bid_volume_1', 0),         # bid_volume
        get_value('turnover', 0.00),          # turnover
        get_value('volume', 0.00),            # volume
        get_value('prev_close'),              # prev_close
        get_value('ask_volume_1', 0),         # ask_volume
        get_value('open_interest'),           # open_interest
        get_value('open_interest_diff', 0),   # open_interest_diff
        get_value('close'),                   # close
    ]

    return row

def export_bars_to_csv(output_file: str, group_id: str, max_messages: int = None):
    """
    从Kafka导出bar数据到CSV文件

    :param output_file: 输出CSV文件路径
    :param group_id: 消费组ID
    :param max_messages: 最大消息数（None表示持续消费）
    """
    # 先获取所有bar_1min主题
    from confluent_kafka.admin import AdminClient
    admin = AdminClient({'bootstrap.servers': bootstrap_servers})
    metadata = admin.list_topics(timeout=10)
    bar_topics = [t for t in metadata.topics.keys() if 'bar_1min_' in t]

    if not bar_topics:
        print("!!! 未找到任何bar_1min主题")
        return

    print(f">>> 找到 {len(bar_topics)} 个bar_1min主题")

    # Kafka消费者配置（增加超时和优化设置）
    consumer_conf = {
        'bootstrap.servers': bootstrap_servers,
        'group.id': group_id,
        'auto.offset.reset': 'earliest',  # 从最早的消息开始
        'enable.auto.commit': True,
        'session.timeout.ms': 300000,      # 5分钟
        'max.poll.interval.ms': 600000,    # 10分钟
        'fetch.wait.max.ms': 500,          # 降低等待时间
        'socket.timeout.ms': 120000        # 2分钟
    }

    consumer = Consumer(consumer_conf)

    # 订阅所有bar_1min主题
    consumer.subscribe(bar_topics)

    print(f">>> 启动消费者，订阅 {len(bar_topics)} 个主题")
    print(f">>> 输出文件: {output_file}")

    # 创建输出目录（如果不存在）
    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    message_count = 0
    rows_buffer = []
    buffer_size = 1000  # 每1000条写入一次

    try:
        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            csv_writer = csv.writer(csvfile)

            # 不写表头（根据参考文件格式）

            while True:
                msg = consumer.poll(timeout=1.0)

                if msg is None:
                    if max_messages and message_count >= max_messages:
                        break
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        print(f"%% {msg.topic()} [{msg.partition()}] reached end at offset {msg.offset()}")
                    else:
                        raise KafkaException(msg.error())
                else:
                    try:
                        # 解析 JSON
                        bar_json = msg.value().decode('utf-8')
                        bar_data = json.loads(bar_json)

                        # 转换为CSV行
                        row = bar_to_csv_row(bar_data)
                        rows_buffer.append(row)

                        message_count += 1

                        # 批量写入
                        if len(rows_buffer) >= buffer_size:
                            csv_writer.writerows(rows_buffer)
                            csvfile.flush()
                            rows_buffer = []
                            print(f"已处理 {message_count} 条消息...")

                        # 达到最大消息数
                        if max_messages and message_count >= max_messages:
                            break

                    except json.JSONDecodeError as e:
                        print(f"!!! JSON 解码失败: {e}，原始数据: {msg.value()}")
                    except Exception as ex:
                        print(f"!!! 处理消息时发生异常: {ex}，bar_data: {bar_data}")

            # 写入剩余数据
            if rows_buffer:
                csv_writer.writerows(rows_buffer)
                csvfile.flush()

        print(f">>> 导出完成！共处理 {message_count} 条消息")
        print(f">>> 输出文件: {output_file}")

    except KeyboardInterrupt:
        print(">>> 消费者被中断，正在保存数据...")
        # 保存剩余数据
        if rows_buffer:
            with open(output_file, 'a', newline='', encoding='utf-8') as csvfile:
                csv_writer = csv.writer(csvfile)
                csv_writer.writerows(rows_buffer)
    finally:
        consumer.close()
        print(f">>> 消费者已关闭，共导出 {message_count} 条数据")

if __name__ == "__main__":
    import sys

    # 默认输出文件（使用今天日期）
    today = datetime.now().strftime('%Y%m%d')
    default_output = f"/root/project/ctp_md_receiver/output/md_snapshot_RTdongzheng.csv.{today}"

    # 解析命令行参数
    output_file = sys.argv[1] if len(sys.argv) > 1 else default_output
    group_id = sys.argv[2] if len(sys.argv) > 2 else "bar-csv-exporter"

    print(f"导出配置:")
    print(f"  输出文件: {output_file}")
    print(f"  消费组ID: {group_id}")
    print(f"  Kafka服务器: {bootstrap_servers}")
    print()

    # 开始导出
    export_bars_to_csv(output_file, group_id)
