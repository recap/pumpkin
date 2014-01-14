__author__ = 'reggie'


###START-CONF
##{
##"object_name": "processor",
##"object_poi": "my-processor-1234",
##"auto-load": true,
##"parameters": [
##                  {
##                      "name": "data",
##                      "description": "file data",
##                      "required": true,
##                      "type": "DataString",
##                      "state" : "RAW"
##                  }
## ],
##"return": [
##              {
##                      "name": "data",
##                      "description": "file data",
##                      "required": true,
##                      "type": "DataString",
##                      "state" : "PROCESSED"
##                  }
##
##          ] }
##END-CONF




from pumpkin import *

class processor(PmkSeed.Seed):

    def __init__(self, context, poi=None):
        PmkSeed.Seed.__init__(self, context,poi)
        pass


    def run(self, pkt, data):
        print "PROCESSOR: "+data
        self.dispatch(pkt, data, "PROCESSED")

        self.ack_pkt(pkt)


        pass

