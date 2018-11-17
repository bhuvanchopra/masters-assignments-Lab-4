from pyspark import SparkConf, SparkContext
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import json

def add_pairs(a,b):
    return (a[0]+b[0],a[1]+b[1])

def relative_score(kv):
    return kv[0]

def output_format(avg_tuple):
    a, b = avg_tuple
    return '[%f %s]' % (a, b)

def main(inputs, output):
    data = sc.textFile(inputs)
    jsondata = data.map(lambda x: json.loads(x)).cache()
    commentbysub = jsondata.map(lambda c: (c['subreddit'], c))
    kvrdd = jsondata.map(lambda x: (x['subreddit'],(1,x['score'])))
    summed = kvrdd.reduceByKey(add_pairs)
    average = summed.map(lambda x: (x[0],x[1][1]/x[1][0]))
    average_positive = average.filter(lambda x: x[1]>0)
    joined_rdd = commentbysub.join(average_positive)
    author_rdd = joined_rdd.map(lambda x: (x[1][0]['score']/x[1][1],x[1][0]['author']))

    outdata = author_rdd.sortBy(relative_score,ascending = False).map(output_format)
    outputdata = outdata.map(lambda x: json.dumps(x))
    outputdata.saveAsTextFile(output)

if __name__ == '__main__':
    conf = SparkConf().setAppName('reddit relative score')
    sc = SparkContext(conf=conf)
    assert sc.version >= '2.3'  # make sure we have Spark 2.3+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)