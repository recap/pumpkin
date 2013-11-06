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

    def run(self):
        automaton = "##START-CONF{\
                        \"invoke\": [\
	                        {\
		    \"func\": \"add\", \
		    \"state\": \"0000\"\
	                        },\
	                    {\
		    \"func\": \"square\", \
		    \"state\": \"0000\" \
	            },\
	            {\
		    \"func\": \"root\",\
		    \"state\": \"0000\"\
	            }\
	            ],\
            \"container-id\": \"qazwsx\",\
            \"box-id\": \"1\"\
} ##END-CONF"

        for p in range (1,5):
            seq = ""
            for j in range(1,10):
                x = randint(2,100)
                seq = seq +str(x)+","

            seq = seq[0:len(seq)-1]
            pkt = automaton + seq
            self.dispatch(seq,"ANY")
            #print pkt
            #self.context.getRx().put(pkt)
        pass