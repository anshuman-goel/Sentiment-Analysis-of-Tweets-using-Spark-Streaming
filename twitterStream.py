from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import operator
import numpy as np
import matplotlib.pyplot as plt
import re


def main():
    conf = SparkConf().setMaster("local[2]").setAppName("Streamer")
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 10)   # Create a streaming context with batch interval of 10 sec
    ssc.checkpoint("checkpoint")

    pwords = load_wordlist("positive.txt")
    nwords = load_wordlist("negative.txt")

    print(pwords)
    
    counts = stream(ssc, pwords, nwords, 100)
    make_plot(counts)

    
def make_plot(counts):
    """
    Plot the counts for the positive and negative words for each timestep.
    Use plt.show() so that the plot will popup.
    """
    # YOUR CODE HERE
    counts=[x for x in counts if x]
    #print('##############################'+str(counts)+'#############################')
    positive=[x[0][1] for x in counts]
    negative=[x[1][1] for x in counts]
    time=len(counts)
    pos=plt.plot(range(0,len(counts)),positive,'bo-',label='positive')
    neg=plt.plot(range(0,len(counts)),negative,'go-',label='negative')
    plt.axis([-1,len(counts),0,max(max(positive),max(negative))+60])
    plt.legend(loc='upper left')
    plt.ylabel('Word Count')
    plt.xlabel('Time Step')
    plt.show()

    

def load_wordlist(filename):
    """ 
    This function should return a list or set of words from the given filename.
    """
    # YOUR CODE HERE
    f=open(filename,'r')
    return set((f.read().split("\n")))


def word_count(word,pwords,nwords):
    if word in pwords:
        return ("positive",1)
    elif word in nwords:
        return ("negative",1)
    else:
        return


def stream(ssc, pwords, nwords, duration):
    kstream = KafkaUtils.createDirectStream(ssc, topics = ['twitterstream'], kafkaParams = {"metadata.broker.list": 'localhost:9092'})
    tweets = kstream.map(lambda x: x[1].encode("ascii","ignore"))

    # Each element of tweets will be the text of a tweet.
    # You need to find the count of all the positive and negative words in these tweets.
    # Keep track of a running total counts and print this at every time step (use the pprint function).
    # YOUR CODE HERE
    #tweets.pprint()
    tweets=tweets.flatMap(lambda x:re.findall(r"[\w']+",x)).map(lambda x:x.lower())
    tweets=tweets.map(lambda x: word_count(x,pwords,nwords)).filter(lambda x: False if x is None else True)
    tweets=tweets.reduceByKey(lambda x,y:x+y)
    tweets.pprint()

    # Let the counts variable hold the word counts for all time steps
    # You will need to use the foreachRDD function.
    # For our implementation, counts looked like:
    #   [[("positive", 100), ("negative", 50)], [("positive", 80), ("negative", 60)], ...]
    counts = []
    # YOURDSTREAMOBJECT.foreachRDD(lambda t,rdd: counts.append(rdd.collect()))
    tweets.foreachRDD(lambda t,rdd: counts.append(rdd.collect()))
    
    ssc.start()                         # Start the computation
    ssc.awaitTerminationOrTimeout(duration)
    ssc.stop(stopGraceFully=True)
    return counts


if __name__=="__main__":
    main()
