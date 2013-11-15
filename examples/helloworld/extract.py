__author__ = 'reggie'

###START-CONF
##{
##"object_name": "extract",
##"object_poi": "my-extract-1234",
##"parameters": [ {
##                  "name": "greeting",
##                  "description": "a greeting",
##                  "required": true,
##                  "type": "String",
##                  "state" : "GREETING"
##              } ],
##"return": []
##}
##END-CONF




from pumpkin import *

class extract(PmkSeed.Seed):

    def __init__(self, context, poi=None):
        PmkSeed.Seed.__init__(self, context,poi)
        pass


    def run(self, pkt, greeting):
        """ An extractor extracts data from the data transformation network.
        In this case the network is simple enough, it only contains one transformer
        seed (greet).
        """
        print "Greeting: "+str(greeting)
        pass