__author__ = 'reggie'


###START-CONF
##{
##"object_name": "filterhasa",
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
##                      "description": "has a relation  tweet",
##                      "required": true,
##                      "type": "TweetString",
##                      "format": "",
##                      "state" : "HASA|RUBBISH"
##                  }
##
##          ] }
##END-CONF



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

from pumpkin import PmkSeed

from random import randint

class filterhasa(PmkSeed.Seed):

    def __init__(self, context, poi=None):
        PmkSeed.Seed.__init__(self, context,poi)
        pass

    def on_load(self):
        print "Loading: " + self.__class__.__name__
        pass


    def run(self, pkt, tweet):
        m = re.search('W(\s+)(.*)(\n)', tweet, re.S)
        if m:
            tw = m.group(2)
            if not self.has_a(tw):
                pass
            else:
                self.dispatch(pkt, self.has_a(tw), "HASA")
            #if self.is_haiku(tw):
            #    self.dispatch(pkt, tweet, "HAIKU")
            #else:
            #    self.dispatch(pkt, tweet, "RUBBSIH")
        pass


    def has_a(self,text):
        m = re.search('([A-Z]+[A-Za-z]+\s*[A-Za-z]*\s(has an|has a)\s[A-Z]+[A-Za-z]+)', text, re.S)
        if m:
            tw = m.group(0)
            return tw
        return False