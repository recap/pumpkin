__author__ = 'reggie'



###START-CONF
##{
##"object_name": "stop",
##"object_poi": "my-stop-1234",
##"auto-load": true,
##"parameters": [
##          {
##                      "name": "name",
##                      "description": "my name",
##                      "required": true,
##                      "type": "String",
##                      "state" : "MET"
##                  }
##
## ],
##"return": [
##
##
##          ] }
##END-CONF




from pumpkin import *

class stop(PmkSeed.Seed):

    def __init__(self, context, poi=None):
        PmkSeed.Seed.__init__(self, context,poi)
        pass

    def on_load(self):
        pass



    def run(self, pkt, data):
        print "RESULT: "+str(data[0])

        pass

