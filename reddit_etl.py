from pyspark import SparkConf, SparkContext
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import json

def e_filter(rdd):
    if 'e' in rdd[0]:
        return rdd

def main(inputs, output):
    data = sc.textFile(inputs)
    jsondata = data.map(lambda x: json.loads(x))
    rdd = jsondata.map(lambda x: (x['subreddit'],x['score'],x['author']))
    rdd1 = rdd.filter(e_filter).cache()
    positive = rdd1.filter(lambda x: x[1]>0)
    negative = rdd1.filter(lambda x: x[1]<=0)

    positive.map(lambda x: json.dumps(x)).saveAsTextFile(output + '/positive')
    negative.map(lambda x: json.dumps(x)).saveAsTextFile(output + '/negative')

if __name__ == '__main__':
    conf = SparkConf().setAppName('reddit ETL')
    sc = SparkContext(conf=conf)
    assert sc.version >= '2.3'  # make sure we have Spark 2.3+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
