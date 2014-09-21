###START-CONF
##{
##"object_name": "sentiment_analyses",
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
##                      "state" : "ENGLISH"
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

import re, os
import urllib2
from random import randint
from pumpkin import PmkSeed
import nltk

class sentiment_analyses(PmkSeed.Seed):

    def __init__(self, context, poi=None):
        PmkSeed.Seed.__init__(self, context,poi)
        self.wd = self.context.getWorkingDir()

    def on_load(self):
        print "Loading: " + self.__class__.__name__
        nltk.data.path.append(self.wd)
        url = "https://github.com/mikolajb/soa-cloud-course/raw/master/assignment02/movie_reviews_NaiveBayes.pickle"
        file_name = self.wd+"movie_reviews_NaiveBayes.pickle"
        file = "movie_reviews_NaiveBayes.pickle"

        self.get_net_file(url, file_name)
        #os.chmod(file_name, 0777)
        self.classifier = nltk.data.load(file)

    def check(self, tweet):
        words = tweet.split()
        feats = dict([(word, True) for word in words])
        return self.classifier.classify(feats) == 'pos'

    def get_net_file(self, url, file_name):
        #file_name = url.split('/')[-1]
        u = urllib2.urlopen(url)
        f = open(file_name, 'wb')
        meta = u.info()
        file_size = int(meta.getheaders("Content-Length")[0])
        self.logger.info ("Downloading: %s Bytes: %s" % (file_name, file_size))

        file_size_dl = 0
        block_sz = 8192
        while True:
            buffer = u.read(block_sz)
            if not buffer:
                break

            file_size_dl += len(buffer)
            f.write(buffer)
            #status = r"%10d  [%3.2f%%]" % (file_size_dl, file_size_dl * 100. / file_size)
            #status = status + chr(8)*(len(status)+1)
            #print status,
        f.close()

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
