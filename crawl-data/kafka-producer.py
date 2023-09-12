import scrapy
import base64
import sys
from kafka import KafkaProducer
KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'MyTopicDemo'

try:
    producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER)
    print("connected!")
except Exception as e:
    print(f'Error Connecting to Kafka --> {e}')
    sys.exit(1)

class QuoteSpider(scrapy.Spider):
    name = 'financeData'
    start_urls = ['https://s.cafef.vn/bao-cao-tai-chinh/VIC/IncSta/2022/4/0/0/ket-qua-hoat-dong-kinh-doanh-tap-doan-vingroup-cong-ty-co-phan.chn',
                  'https://s.cafef.vn/bao-cao-tai-chinh/FPT/IncSta/2022/4/0/0/ket-qua-hoat-dong-kinh-doanh-cong-ty-co-phan-fpt.chn']

    def parse(self, response):
        financeData = []
        flag = False
        k = 0
        message = '' 
        cpname = response.css('#txtKeyword::attr(value)').extract()
        data = response.css('tr td::text').extract()
        for d in data: 
            message = message + d + '$'
        message = cpname[0] + message
        message_bytes = message.encode('utf-8')
        producer.send(KAFKA_TOPIC, message_bytes)
        print('Message sent: ',message_bytes)
        yield {
            'msg': message
        }