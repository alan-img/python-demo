# -*- coding: utf-8 -*-
import json
import sys
import threading
import time
from datetime import datetime

import requests
from kafka import KafkaProducer

if len(sys.argv) > 1:
    request_delay_seconds = int(sys.argv[1])
else:
    request_delay_seconds = 1

API_KEY = "SYB93XLvV_2i1PrpH"
BASE_URL = "https://api.seniverse.com/v3/weather/now.json"
bootstrap_servers = ["hadoop101:9092", "hadoop102:9092", "hadoop103:9092"]
topic_name = "weather_info"
producer = KafkaProducer(bootstrap_servers=bootstrap_servers, value_serializer=lambda v: v.encode("UTF-8"))
cities = [
    "Beijing", "Shanghai", "Guangzhou", "Shenzhen", "Chengdu",
    "Wuhan", "Hangzhou", "Nanjing", "Tianjin", "Chongqing"
]


def get_cities_weather(request_delay_seconds):
    weather_info_dct = {}

    for city in cities:
        try:
            params = {
                "key": API_KEY,
                "location": city,
                "language": "zh-Hans",
                "unit": "c"
            }
            resp = requests.get(BASE_URL, params=params, timeout=5)
            resp.raise_for_status()
            data = resp.json()
            now = data["results"][0]["now"]

            weather = now["text"]
            temperature = now["temperature"]

            now = datetime.now()
            create_time = now.strftime("%Y-%m-%d %H:%M:%S")

            weather_info_dct["city"] = city
            weather_info_dct["weather"] = weather
            weather_info_dct["temperature"] = temperature
            weather_info_dct["create_time"] = create_time
            weather_info = json.dumps(weather_info_dct, ensure_ascii=False)
            # print("weather_info: {}".format(weather_info))
            producer.send(topic_name, weather_info)

            # 避免触发 API 限速
            time.sleep(request_delay_seconds)

        except Exception as e:
            print(f"❌ 获取 {city} 天气失败: {e}")


def loop_fetch(request_delay_seconds):
    while True:
        get_cities_weather(request_delay_seconds)


if __name__ == "__main__":
    print(f"每秒获取配置的列表城市天气信息...")
    threading.Thread(target=loop_fetch(request_delay_seconds), daemon=True).start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("退出天气爬虫")

    producer.flush()
    producer.close()
