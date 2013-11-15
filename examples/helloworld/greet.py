__author__ = 'reggie'

###START-CONF
##{
##"object_name": "greet",
##"object_poi": "my-greet-1234",
##"auto-load" : true,
##"parameters": [ {
##                  "name": "name",
##                  "description": "name to greet",
##                  "required": true,
##                  "type": "String",
##                  "state" : "UNGREETED"
##              } ],
##"return": [
##              {
##                  "name": "greeting",
##                  "description": "a greeting",
##                  "required": true,
##                  "type": "String",
##                  "state" : "GREETING"
##               }
##
##          ] }
##END-CONF




from pumpkin import *

class greet(PmkSeed.Seed):

    def __init__(self, context, poi=None):
        PmkSeed.Seed.__init__(self, context,poi)
        pass


    def run(self, pkt, name):
        """ Data is transformed at intermediate points on its way
        to a destination. In this case we are simply adding
        "hello" to a name to form a greeting. This will be
        dispatched and received by a collector.
        """
        greeting = "Hello " + str(name)
        self.dispatch(pkt, greeting, "GREETING")
        pass