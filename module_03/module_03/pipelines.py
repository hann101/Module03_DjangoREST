# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

from kafka import KafkaProducer
from json import dumps
import time
# useful for handling different item types with a single interface
# from itemadapter import ItemAdapter

class Module03Pipeline(object):
    def __init__(self):
        self.producer = KafkaProducer(acks=0, 
                        compression_type='gzip',
                        bootstrap_servers=['localhost:9092'],
                        value_serializer=lambda x :dumps(x).encode('utf-8')
                        # print("dbdbdbdbdbdbdbdbdbdbdbdbdbdbdbdbdbdbdbdbdb")
                        # dumps 파이썬의 dict를 json형태로 바꿔줌
                        # bite를 원래형태로 할때는 역직렬화
                        # 직렬화는 json타입으로 전달해줘야한다.
        )
    def process_item(self, item, spider):
        
        item = dict(item)
        # print(item)

        
        data = {"schema":{"type":"struct","fields":[{"type":"int32","optional":False,"field":"id"},{"type":"string","optional":False,"field":"title"},{"type":"string","optional":False,"field":"weekend"},{"type":"string","optional":False,"field":"gross"},{"type":"string","optional":False,"field":"weeks"},{"type":"string","optional":False,"field":"genre"},{"type":"string","optional":False,"field":"rating"},{"type":"string","optional":False,"field":"movie_release"},{"type":"string","optional":False,"field":"people"},{"type":"int64","optional":True,"name":"org.apache.kafka.connect.data.Timestamp","version":1,"field":"created_at"}],"optional":False,"name":"movie"},"payload":{"id":4,"title":item['title'],"weekend":item['weekend'],"gross":item['gross'],"weeks":item['weeks'],"genre":item['genre'],"rating":item['rating'],"movie_release":item['movie_release'],"people":item['people'],"created_at":int(time.time()*1000)}}





        # data = {"schema":{"type":"struct","fields":[{"type":"string","field":"title"},{"type":"string","field":"weekend"},{"type":"string","field":"gross"},{"type":"string","field":"weeks"},{"type":"string","field":"genre"},{"type":"string","field":"rating"},{"type":"string","field":"movie_release"},{"type":"string","field":"people"},{"type":"int64","name":"org.apache.kafka.connect.data.Timestamp","version":1,"field":"created_at"}],"name":"users"},"payload":{"title":item['title'],"weekend":item['weekend'],"gross":item['gross'],"weeks":item['weeks'],"genre":item['genre'],"rating":item['rating'],"movie_release":item['movie_release'],"people":item['people'],"created_at":int(time.time()*1000)}}


        # {"schema":{"type":"struct","fields":[{"type":"int32","optional":false,"field":"id"},{"type":"string","optional":false,"field":"title"},{"type":"string","optional":false,"field":"weekend"},{"type":"string","optional":false,"field":"gross"},{"type":"string","optional":false,"field":"weeks"},{"type":"string","optional":false,"field":"genre"},{"type":"string","optional":false,"field":"rating"},{"type":"string","optional":false,"field":"movie_release"},{"type":"string","optional":false,"field":"people"},{"type":"int64","optional":true,"name":"org.apache.kafka.connect.data.Timestamp","version":1,"field":"created_at"}],"optional":false,"name":"movie"},"payload":{"id":4,"title":"kkk","weekend":"kkkk","gross":"kkk","weeks":"kkkk","genre":"kkk","rating":"kkk","movie_release":"kkkk","people":"kkkkkk","created_at":1615949031000}}


        # data = {"schema":{"type":"struct","fields":[{"type":"string","field":"title"},{"type":"string","field":"weekend"},{"type":"string","field":"gross"},{"type":"string","field":"weeks"},{"type":"string","field":"genre"},{"type":"string","field":"rating"},{"type":"string","field":"movie_release"},{"type":"string","field":"people"},{"type":"int64","name":"org.apache.kafka.connect.data.Timestamp","version":1,"field":"created_at"}],"name":"users"},"payload":{"title":item['title'],"weekend":item['weekend'],"gross":item['gross'],"weeks":item['weeks'],"genre":item['genre'],"rating":item['rating'],"movie_release":item['movie_release'],"people":item['people'],"created_at":int(time.time()*1000)}}



        self.producer.send('my_topic_movie', value = data)
        time.sleep(10)
        self.producer.flush()
        print('Done:')
 
        # print('*********************',item,'*********************')
        # print('*********************',items,'*********************')



#{"schema":{"type":"struct","fields":[{"type":"string","field":"title"},{"type":"string","field":"weekend"},{"type":"string","field":"gross"},{"type":"string","field":"weeks"},,{"type":"string","field":"genre"},{"type":"string","field":"rating"},{"type":"string","field":"movie_release"},{"type":"string","field":"people"}],"name":"users"},{"type":"int64","name":"org.apache.kafka.connect.data.Timestamp","version":1,"field":"created_at"},"payload":{"title":"kkkkk","weekend":"kkk","gross":"akkaa","weeks":"akkaa","genre":"abb135kkkk43aa","rating":"aakkkaaaaaaaa","movie_release":"akkkaa","people":"aabkkkbba","created_at":1615860192000}}

# 스키마
# {"schema":{"type":"struct","fields":[{"type":"int32","optional":false,"field":"id"},{"type":"string","optional":false,"field":"title"},{"type":"string","optional":false,"field":"weekend"},{"type":"string","optional":false,"field":"gross"},{"type":"string","optional":false,"field":"weeks"},{"type":"string","optional":false,"field":"genre"},{"type":"string","optional":false,"field":"rating"},{"type":"string","optional":false,"field":"movie_release"},{"type":"string","optional":false,"field":"people"},{"type":"int64","optional":true,"name":"org.apache.kafka.connect.data.Timestamp","version":1,"field":"created_at"}],"optional":false,"name":"movie"},"payload":{"id":4,"title":"rrr","weekend":"rrr","gross":"rrr","weeks":"rrrr","genre":"rrr","rating":"rrr","movie_release":"rrr","people":"rrrrr","created_at":1615949031000}}
