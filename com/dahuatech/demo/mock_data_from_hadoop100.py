# -*- coding: utf-8 -*-
import sys
import threading
import time

import requests
from kafka import KafkaProducer

if len(sys.argv) > 1:
    request_delay_seconds = sys.argv[1]
else:
    request_delay_seconds = 1

BASE_URL = "http://hadoop100:18080/weather/{}"

bootstrap_servers = ["hadoop101:9092", "hadoop102:9092", "hadoop103:9092"]
topic_name = "weather_info"
producer = KafkaProducer(bootstrap_servers=bootstrap_servers, value_serializer=lambda v: v.encode("UTF-8"))
cities = [
    "Beijing", "Shanghai", "Guangzhou", "Shenzhen", "Chengdu",
    "Wuhan", "Hangzhou", "Nanjing", "Tianjin", "Chongqing"
]


def get_cities_weather():
    for city in cities:
        try:
            url = BASE_URL.format(city)
            resp = requests.get(url, timeout=5)
            resp.raise_for_status()

            producer.send(topic_name, resp.text)

            # 避免触发 API 限速
            time.sleep(float(request_delay_seconds))
        except Exception as e:
            print(f"❌ 获取 {city} 天气失败: {e}")


def loop_fetch():
    while True:
        get_cities_weather()


if __name__ == "__main__":
    print(f"每秒获取配置的列表城市天气信息...")
    threading.Thread(target=loop_fetch(), daemon=True).start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("退出天气爬虫")

    producer.flush()
    producer.close()
