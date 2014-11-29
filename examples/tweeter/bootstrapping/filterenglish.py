__author__ = 'reggie'


###START-CONF
##{
##"object_name": "filterenglish",
##"object_poi": "qpwo-2345",
##"parameters": [
##                 {
##                      "name": "tweet",
##                      "description": "raw numbers",
##                      "required": true,
##                      "type": "TweetString",
##                      "format": "",
##                      "state" : "RAW"
##                  }
##              ],
##"return": [
##              {
##                      "name": "tweet",
##                      "description": "english or non-english tweet",
##                      "required": true,
##                      "type": "TweetString",
##                      "format": "",
##                      "state" : "ENGLISH|NONENGLISH"
##                  }
##
##          ] }
##END-CONF



import re
import nltk
import time
import sys

from pumpkin import *



class filterenglish(PmkSeed.Seed):

    def __init__(self, context, poi=None):
        PmkSeed.Seed.__init__(self, context,poi)
        self.eng_cnt = 0
        self.neng_cnt = 0
        self.count = 0


        pass

    def on_load(self):
        print "Loading: " + self.__class__.__name__
        wd = self.context.getWorkingDir()
        nltk.data.path.append(wd + "nltk_data")


        self.ENGLISH_STOPWORDS = set(nltk.corpus.stopwords.words('english'))
        self.NON_ENGLISH_STOPWORDS = set(nltk.corpus.stopwords.words()) - self.ENGLISH_STOPWORDS

        self.tm = time.time()

        pass

    def is_english(self, text):

        text = text.lower()
        words = set(nltk.wordpunct_tokenize(text))
        return len(words & self.ENGLISH_STOPWORDS) > len(words & self.NON_ENGLISH_STOPWORDS)

    def run(self, pkt, data):
        try:
            pkt = Packet.set_streaming_bits(pkt)
            time.sleep(0.1)
            tweet = data[0]
            m = re.search('W(\s+)(.*)(\n)', tweet, re.S)
            if m:
                #self.count += 1
                #if self.count >= 100000:
                #    self.count = 0
                #    ts = time.time()
                #    epoch = ts - self.tm
                #    total = self.eng_cnt + self.neng_cnt
                #    per = (self.eng_cnt * 100) / total
                #    #print "milestone: eng ["+str(self.eng_cnt)+", "+str(per)+"%] non-eng ["+str(self.neng_cnt)+"] total ["+str(total)+"] el_time ["+str(epoch)+"]"

                tw = m.group(2)
                if self.is_english(tw):
                    #self.dispatch(pkt, tweet, "ENGLISH")
                    #self.eng_cnt += 1
                    print "English"
                    pass
                else:
                    print "NON ENGLISH"
                    #self.dispatch(pkt, tweet, "NONENGLISH")
                    #self.neng_cnt += 1
                    #print "Non English"
                    pass
        except:
            self.logger.error("Unexpected error:" + str(sys.exc_info()[0]))

        pass
