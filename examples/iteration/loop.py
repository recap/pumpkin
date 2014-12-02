__author__ = 'reggie'
__author__ = 'reggie'


###START-CONF
##{
##"object_name": "loop",
##"object_poi": "my-loop-1234",
##"auto-load": true,
##"parameters": [
##              {
##                      "name": "name",
##                      "description": "my name",
##                      "required": true,
##                      "type": "String",
##                      "state" : "NOT_MET"
##                  }
## ],
##"return": [
##              {
##                      "name": "name",
##                      "description": "my name",
##                      "required": true,
##                      "type": "String",
##                      "state" : "READY"
##                  }
##
##          ] }
##END-CONF




from pumpkin import *

class loop(PmkSeed.Seed):

    def __init__(self, context, poi=None):
        PmkSeed.Seed.__init__(self, context,poi)
        pass

    def on_load(self):
        pass

    def run(self, pkt, data):
        print "PROCESSING DATA"

        x = int(data[0])
        x += 2
        self.fork_dispatch(pkt,str(x),"READY")

        pass

