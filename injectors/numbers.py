__author__ = 'reggie'

###START-CONF
##{
##"object_name": "numbers",
##"object_poi": "qpwo-2345-qw-212",
##"parameters": [
##
##              ],
##"return": [
##              {
##                      "name": "number_list",
##                      "description": "csv of numbers",
##                      "required": true,
##                      "type": "CSVStringNumbers",
##                      "format": "csv",
##                      "state" : "LONG|SHORT|MEDIUM|NOSTATE"
##                  }
##
##          ] }
##END-CONF

import DRPlugin
import DRShared
import time



from random import randint

class numbers(DRPlugin.PluginBase):
    def __init__(self, context, poi=None):
        DRPlugin.PluginBase.__init__(self, context,poi)
        #self.istate["sequence_size"] = "ANY"
        #self.ostate["number_size"] = "SMALL|BIG"
        pass

    def on_load(self):
        print "Loading: " + self.__class__.__name__
        pass

    def run(self, pkt):

        for p in range (1,5):
            seq = ""
            for j in range(1,10):
                x = randint(2,100)
                seq = seq +str(x)+","

            seq = seq[0:len(seq)-1]
            self.dispatch(pkt, seq,"SHORT")
            #print pkt
            #self.context.getRx().put(pkt)
        pass