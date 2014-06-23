###START-CONF
##{
##"object_name": "sp_tweetinject",
##"object_poi": "qpwo-2345",
##"auto-load": true,
##"remoting" : false,
##"parameters": [
##
##              ],
##"return": [
##              {
##                      "name": "tweet",
##                      "description": "raw tweet",
##                      "required": true,
##                      "type": "TweetString",
##                      "format": "",
##                      "state" : "RAW"
##                  }
##
##          ] }
##END-CONF


import re
import nltk
import time

from os import listdir
from os.path import isfile, join
import pika
from os.path import expanduser

from pumpkin import *

class sp_tweetinject(PmkSeed.Seed):

    def __init__(self, context, poi=None):
        PmkSeed.Seed.__init__(self, context,poi)
        self.eng_cnt = 0
        self.neng_cnt = 0
        self.count = 0


    def on_load(self):
        print "Loading: " + self.__class__.__name__

        wd = self.context.getWorkingDir()
        nltk.data.path.append(wd + "nltk_data")


        self.ENGLISH_STOPWORDS = set(nltk.corpus.stopwords.words('english'))
        self.NON_ENGLISH_STOPWORDS = set(nltk.corpus.stopwords.words()) - self.ENGLISH_STOPWORDS

        self.tm = time.time()

    def is_english(self, text):

        text = text.lower()
        words = set(nltk.wordpunct_tokenize(text))
        return len(words & self.ENGLISH_STOPWORDS) > len(words & self.NON_ENGLISH_STOPWORDS)

    def fil_run(self, pkt, data):
        tweet = data
        m = re.search('W(\s+)(.*)(\n)', tweet, re.S)
        if m:
            self.count += 1
            if self.count >= 100000:
                self.count = 0
                ts = time.time()
                epoch = ts - self.tm
                total = self.eng_cnt + self.neng_cnt
                per = (self.eng_cnt * 100) / total
                print "milestone: eng ["+str(self.eng_cnt)+", "+str(per)+"%] non-eng ["+str(self.neng_cnt)+"] total ["+str(total)+"] el_time ["+str(epoch)+"]"

            tw = m.group(2)
            if self.is_english(tw):
                #self.dispatch(pkt, tweet, "ENGLISH")
                self.eng_cnt += 1
            else:
                #self.dispatch(pkt, tweet, "NONENGLISH")
                self.neng_cnt += 1
        pass

    def run(self, pkt):
        dir = expanduser("~")+"/tweets/"
        onlyfiles = [ f for f in listdir(dir) if isfile(join(dir,f)) ]
        for fl in onlyfiles:
            fullpath = dir+fl
            if( fl[-3:] == "txt"):
                print "File: "+str(fl)
                with open(fullpath) as f:
                    for line in f:
                        if line.startswith('T'):
                            tweet = line
                        if line.startswith("U"):
                            tweet = tweet + line
                        if line.startswith("W"):
                            if line == "No Post Title":
                                line =""
                            else:
                                tweet = tweet + line
                                self.fil_run(pkt, tweet)
                                #self.dispatch(pkt, tweet, "RAW")
                                del line
                                del tweet


