__author__ = 'reggie'



###START-CONF
##{
##"object_name": "master",
##"object_poi": "my-master-1234",
##"auto-load": true,
##"parameters": [
##              {
##                      "name": "name",
##                      "description": "my name",
##                      "required": true,
##                      "type": "String",
##                      "state" : "PROCESSED"
##                  }
## ],
##"return": [
##              {
##                      "name": "name",
##                      "description": "my name",
##                      "required": true,
##                      "type": "String",
##                      "state" : "2_BE_PROCESSED"
##                  }
##
##          ] }
##END-CONF




from pumpkin import *

class master(PmkSeed.Seed):

    def __init__(self, context, poi=None):
        PmkSeed.Seed.__init__(self, context,poi)
        pass

    def on_load(self):
        t1 = threading.Thread(target=self.inject)
        t1.start()
        pass

    def inject(self):
        pkt = self.get_empty_packet()
        print "SENDING DATA..."
        data = "some data"
        self.fork_dispatch(pkt, data, "2_BE_PROCESSED")


    def run(self, pkt, data):
        print "RESULT: "+str(data[0])
        pass

