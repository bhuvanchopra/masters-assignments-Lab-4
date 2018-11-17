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

def joined(elem,bcast):
    return (elem[1]['score']/bcast.value[elem[0]],elem[1]['author'])

def main(inputs, output):
    data = sc.textFile(inputs)
    jsondata = data.map(lambda x: json.loads(x)).cache()
    commentbysub = jsondata.map(lambda c: (c['subreddit'], c))
    summed = jsondata.map(lambda x: (x['subreddit'],(1,x['score']))).reduceByKey(add_pairs)
    average = summed.map(lambda x: (x[0],x[1][1]/x[1][0]))
    average_positive = average.filter(lambda x: x[1]>0)
    #The following command: .collect() is making a list of all the averages of the subreddits.
    #The length of this list is not too long as it is storing values as averages of unique subreddits name. In case of reddit-1, its length is only 5. 
    broadcast = sc.broadcast(dict(average_positive.collect()))
    author_rdd = commentbysub.map(lambda x: joined(x,broadcast))

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