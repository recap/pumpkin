__author__ = 'reggie'



###START-CONF
##{
##"object_name": "start",
##"object_poi": "my-start-1234",
##"auto-load": true,
##"parameters": [
##
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

class start(PmkSeed.Seed):

    def __init__(self, context, poi=None):
        PmkSeed.Seed.__init__(self, context,poi)
        pass

    def on_load(self):
        pass



    def run(self, pkt):
        print "STARTING WITH X: 0"
        x = 0
        self.fork_dispatch(pkt, str(x), "READY")
        pass

