import time
import json
from pyspark import SparkContext, SQLContext
from pyspark.streaming import StreamingContext
from collections import namedtuple

TagCount = namedtuple('TagCount',("tag","count"))

def gettext(tweetjson):
    if ('text' in tweetjson):
        return tweetjson['text']
    else:
        return ''


def main():
    # SparkContext(“local[1]”) would not work with Streaming bc 2 threads are required

    sc = SparkContext(appName="Twitter Demo")
    ssc = StreamingContext(sc, 10)  # 10 is the batch interval in seconds

    datastream = ssc.socketTextStream("localhost", 5555)

    (datastream
            .map(lambda tweet: gettext(json.loads(tweet)))
            .flatMap(lambda text: text.split(" "))
            .filter(lambda word: word.startswith("#"))
            .map(lambda word: (word.lower(), 1))
            .reduceByKey(lambda a,b: a+b)
            .map(lambda rec:TagCount(rec[0],rec[1]))
            .foreachRDD(lambda rdd: rdd.toDF().registerTempTable("tag_counts"))
     )

    ssc.start()

    sqlc = SQLContext(sc)
    cnt = 0
    while cnt < 100:
        time.sleep(20)
        cnt = cnt + 1
        top_hash_tag = sqlc.sql('select tag,count from tag_counts order by count desc limit 10')
        for r in top_hash_tag.collect():
            print(r.tag, r['count'])
        print('---------------------')


    ssc.awaitTermination()



if __name__ == "__main__":
    main()

