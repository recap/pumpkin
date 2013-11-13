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




from pumpkin import PmkSeed

import json
import re
import networkx as nx
from networkx.readwrite import json_graph


class collectorisa(PmkSeed.Seed):

    def __init__(self, context, poi=None):
        PmkSeed.Seed.__init__(self, context,poi)
        self.d = None
        self.G = nx.DiGraph()
        pass

    def on_load(self):
        print "Loading: " + self.__class__.__name__

        pass


    def run(self, pkt, tweet):
        st = tweet.replace("is an", "is a")
        sta = st.split("is a")
        self.G.add_edge(sta[0].strip(),sta[1].strip())

        pass

    def serve(self):
        d = json_graph.node_link_data(self.G)
        return json.dumps(d)
