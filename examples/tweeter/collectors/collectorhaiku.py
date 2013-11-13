__author__ = 'reggie'


###START-CONF
##{
##"object_name": "collectorhaiku",
##"object_poi": "qpwo-2345",
##"parameters": [
##                 {
##                      "name": "tweet",
##                      "description": "english haiku tweets",
##                      "required": true,
##                      "type": "TweetString",
##                      "format": "",
##                      "state" : "HAIKU"
##                  }
##              ],
##"return": [
##
##          ] }
##END-CONF



import re


from pumpkin import PmkSeed

import json
import re
import networkx as nx
from networkx.readwrite import json_graph



class collectorhaiku(PmkSeed.Seed):

    def __init__(self, context, poi=None):
        PmkSeed.Seed.__init__(self, context,poi)
        self.h = []
        pass

    def on_load(self):
        print "Loading: " + self.__class__.__name__

        pass


    def run(self, pkt, tweet):
        #print "RECEIVED TWEET: "+tweet
        m = re.search('W(\s+)(.*)(\n)', tweet, re.S)
        if m:
            tw = m.group(2)
            print "HAIKU: "+str(tw)
            self.h.append(str(tw))
        pass

    def serve(self):
        d = json.dumps(self.h)
        return str(d)