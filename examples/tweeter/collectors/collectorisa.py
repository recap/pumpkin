__author__ = 'reggie'


###START-CONF
##{
##"object_name": "collectorisa",
##"object_poi": "qpwo-2345",
##"parameters": [
##                 {
##                      "name": "tweet",
##                      "description": "english haiku tweets",
##                      "required": true,
##                      "type": "TweetString",
##                      "format": "",
##                      "state" : "ISA"
##                  }
##              ],
##"return": [
##
##          ] }
##END-CONF


import time
import os
import json
import re
from os import listdir
from os.path import isfile, join

from pumpkin import PmkSeed


from random import randint

class collectorisa(PmkSeed.Seed):

    def __init__(self, context, poi=None):
        PmkSeed.Seed.__init__(self, context,poi)
        self.d = None
        pass

    def on_load(self):
        print "Loading: " + self.__class__.__name__

        pass


    def run(self, pkt, tweet):
        #print "RECEIVED TWEET: "+tweet
        #m = re.search('W(\s+)(.*)(\n)', tweet, re.S)
        #if m:
        #    tw = m.group(2)
        print "ISA: "+str(tweet)

        pass

