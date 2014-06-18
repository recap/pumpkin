__author__ = 'reggie'


###START-CONF
##{
##"object_name": "dumper",
##"object_poi": "qpwo-2345",
##"parameters": [
##                 {
##                      "name": "tweet",
##                      "description": "",
##                      "required": true,
##                      "type": "TweetString",
##                      "format": "",
##                      "state" : "ENGLISH|NONENGLISH"
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
        pass

    def on_load(self):
        print "Loading: " + self.__class__.__name__
        pass


    def run(self, pkt, tweet):
        pass
