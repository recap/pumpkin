__author__ = 'reggie'



###START-CONF
##{
##"object_name": "condition",
##"object_poi": "my-condition-1234",
##"auto-load": true,
##"parameters": [
##              {
##                      "name": "name",
##                      "description": "my name",
##                      "required": true,
##                      "type": "String",
##                      "state" : "READY"
##                  }
## ],
##"return": [
##              {
##                      "name": "name",
##                      "description": "my name",
##                      "required": true,
##                      "type": "String",
##                      "state" : "MET|NOT_MET"
##                  }
##
##          ] }
##END-CONF




from pumpkin import *

class condition(PmkSeed.Seed):

    def __init__(self, context, poi=None):
        PmkSeed.Seed.__init__(self, context,poi)
        self.condition = 100
        pass

    def on_load(self):

        pass




    def run(self, pkt, data):
        x = int(data[0])
        if x is 100:
            print "CONDITION MET"
            self.fork_dispatch(pkt, str(x), "MET")
        else:
            print "CONDITION NOT MET: "+str(x)
            self.fork_dispatch(pkt, str(x), "NOT_MET")
        pass

