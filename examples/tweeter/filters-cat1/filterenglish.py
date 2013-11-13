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

from pumpkin import PmkSeed



class filterenglish(PmkSeed.Seed):

    def __init__(self, context, poi=None):
        PmkSeed.Seed.__init__(self, context,poi)

        pass

    def on_load(self):
        print "Loading: " + self.__class__.__name__

        pass

    def is_english(self, text):

        nltk.data.path.append("./examples/tweeter/nltk_data")
        ENGLISH_STOPWORDS = set(nltk.corpus.stopwords.words('english'))
        NON_ENGLISH_STOPWORDS = set(nltk.corpus.stopwords.words()) - ENGLISH_STOPWORDS

        text = text.lower()
        words = set(nltk.wordpunct_tokenize(text))
        return len(words & ENGLISH_STOPWORDS) > len(words & NON_ENGLISH_STOPWORDS)

    def run(self, pkt, tweet):
        #print "RECEIVED TWEET: "+tweet
        m = re.search('W(\s+)(.*)(\n)', tweet, re.S)
        if m:
            tw = m.group(2)
            if self.is_english(tw):
                self.dispatch(pkt, tweet, "ENGLISH")
            #else:
            #    self.dispatch(pkt, tweet, "NONENGLISH")
        pass
