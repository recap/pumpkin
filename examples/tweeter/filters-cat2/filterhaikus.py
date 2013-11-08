__author__ = 'reggie'


###START-CONF
##{
##"object_name": "filterhaikus",
##"object_poi": "qpwo-2345",
##"parameters": [
##                 {
##                      "name": "tweet",
##                      "description": "english tweets",
##                      "required": true,
##                      "type": "TweetString",
##                      "format": "",
##                      "state" : "ENGLISH"
##                  }
##              ],
##"return": [
##              {
##                      "name": "tweet",
##                      "description": "haiku tweet",
##                      "required": true,
##                      "type": "TweetString",
##                      "format": "",
##                      "state" : "HAIKU|RUBBISH"
##                  }
##
##          ] }
##END-CONF


import DRPlugin
import DRShared
import time
import os
import json
import re
from os import listdir
from os.path import isfile, join
import nltk
from nltk.corpus import cmudict
from curses.ascii import isdigit
import re



from random import randint

class filterhaikus(DRPlugin.PluginBase):

    def __init__(self, context, poi=None):
        DRPlugin.PluginBase.__init__(self, context,poi)
        self.d = None
        pass

    def on_load(self):
        print "Loading: " + self.__class__.__name__
        nltk.data.path.append("./examples/tweeter/nltk_data")
        self.d =  cmudict.dict()
        pass


    def run(self, pkt, tweet):
        #print "RECEIVED TWEET: "+tweet
        m = re.search('W(\s+)(.*)(\n)', tweet, re.S)
        if m:
            tw = m.group(2)
            if self.is_haiku(tw):
                self.dispatch(pkt, tweet, "HAIKU")
            #else:
            #    self.dispatch(pkt, tweet, "RUBBSIH")
        pass

    def is_haiku(self, text):

        text_orig = text
        text = text.lower()
        if filter(str.isdigit, str(text)):
            return False
        words = nltk.wordpunct_tokenize(re.sub('[^a-zA-Z_ ]', '',text))
        #print words
        syl_count = 0
        word_count = 0
        haiku_line_count = 0
        lines = []
        d = self.d

        for word in words:
            if word.lower() in d.keys():
                syl_count += [len(list(y for y in x if isdigit(y[-1]))) for x in
                        d[word.lower()]][0]
            if haiku_line_count == 0:
                if syl_count == 5:
                    lines.append(word)
                    haiku_line_count += 1
            elif haiku_line_count == 1:
                if syl_count == 12:
                    lines.append(word)
                    haiku_line_count += 1
            else:
                if syl_count == 17:
                    lines.append(word)
                    haiku_line_count += 1

        if syl_count == 17:
            try:
                final_lines = []

                str_tmp = ""
                counter = 0
                for word in text_orig.split():
                    str_tmp += str(word) + " "
                    if lines[counter].lower() in str(word).lower():
                        final_lines.append(str_tmp.strip())
                        counter += 1
                        str_tmp = ""
                if len(str_tmp) > 0:
                    final_lines.append(str_tmp.strip())
                return True

            except Exception as e:
                print e
                return False
        else:
            return False

        return True

