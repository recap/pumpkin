__author__ = 'reggie'


###START-CONF
##{
##"object_name": "collectorhasa",
##"object_poi": "qpwo-2345",
##"parameters": [
##                 {
##                      "name": "tweet",
##                      "description": "english haiku tweets",
##                      "required": true,
##                      "type": "TweetString",
##                      "format": "",
##                      "state" : "HASA"
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

class collectorhasa(PmkSeed.Seed):

    def __init__(self, context, poi=None):
        PmkSeed.Seed.__init__(self, context,poi)
        self.G = nx.DiGraph()
        pass

    def on_load(self):
        print "Loading: " + self.__class__.__name__

        pass


    def run(self, pkt, tweet):
        st = tweet.replace("has an", "has a")
        sta = st.split("has a")
        self.G.add_edge(sta[0].strip(),sta[1].strip())

        pass


    def serve(self):
        d = json_graph.node_link_data(self.G)
        return json.dumps(d)
