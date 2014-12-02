__author__ = 'reggie'
__author__ = 'reggie'


###START-CONF
##{
##"object_name": "slave",
##"object_poi": "my-slave-1234",
##"auto-load": true,
##"parameters": [
##              {
##                      "name": "name",
##                      "description": "my name",
##                      "required": true,
##                      "type": "String",
##                      "state" : "2_BE_PROCESSED"
##                  }
## ],
##"return": [
##              {
##                      "name": "name",
##                      "description": "my name",
##                      "required": true,
##                      "type": "String",
##                      "state" : "PROCESSED"
##                  }
##
##          ] }
##END-CONF




from pumpkin import *

class slave(PmkSeed.Seed):

    def __init__(self, context, poi=None):
        PmkSeed.Seed.__init__(self, context,poi)
        pass

    def on_load(self):

        pass





    def run(self, pkt, data):
        print "PROCESSING DATA"
        reversed_string = data[0][::-1]
        self.fork_dispatch(pkt,reversed_string,"PROCESSED")

        pass

