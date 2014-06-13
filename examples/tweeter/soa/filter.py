###START-CONF
##{
##"object_name": "filter",
##"object_poi": "qpwo-2345",
##"auto-load": true,
##"remoting" : true,
##"parameters": [
##                 {
##                      "name": "tweet",
##                      "description": "",
##                      "required": true,
##                      "type": "TweetString",
##                      "format": "",
##                      "state" : "RAW"
##                  }
##              ],
##"return": [
##              {
##                      "name": "tweet",
##                      "description": "sentiment analysis",
##                      "required": true,
##                      "type": "TweetString",
##                      "format": "",
##                      "state" : "POSITIVE|NEGATIVE"
##                  }
##
##          ] }
##END-CONF

import re
from random import randint
from pumpkin import PmkSeed
import nltk

class filter(PmkSeed.Seed):

    def __init__(self, context, poi=None):
        PmkSeed.Seed.__init__(self, context,poi)

    def on_load(self):
        print "Loading: " + self.__class__.__name__
        self.classifier = nltk.data.load("classifiers/movie_reviews_NaiveBayes.pickle")

    def check(self, tweet):
        words = tweet.split()
        feats = dict([(word, True) for word in words])
        return self.classifier.classify(feats) == 'pos'

    def some_check(self, tweet):
        x = randint(0,1)
        if x == 0:
            return True
        else:
            return False

    def run(self, pkt, tweet):
        m = re.search('W(\s+)(.*)(\n)', tweet, re.S)
        if m:
            tw = m.group(2)
            if self.check(tw):
               self.dispatch(pkt, tweet, "POSITIVE")
            else:
               self.dispatch(pkt, tweet, "NEGATIVE")
