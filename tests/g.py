__author__ = 'reggie'

###START-CONF
##{
##"object_name": "mars",
##"object_poi": "my-mars-1234",
##"auto-load" : true,
##"parameters": [ {
##                  "name": "name",
##                  "description": "name to mars",
##                  "required": true,
##                  "type": "X",
##                  "state" : "S1"
##              } ],
##"return": [
##              {
##                  "name": "greeting",
##                  "description": "a greeting",
##                  "required": true,
##                  "type": "X",
##                  "state" : "S2.[0,9]"
##               }
##
##          ] }
##END-CONF




from pumpkin import *

class g(PmkSeed.Seed):

    def __init__(self, context, poi=None):
        PmkSeed.Seed.__init__(self, context,poi)
        pass


    def run(self, pkt, data):
        """
        """
        print "in g()"
        #self.dispatch(pkt, None, "EARTH")



        pass
