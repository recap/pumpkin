__author__ = 'reggie'


###START-CONF
##{
##"object_name": "receiver",
##"object_poi": "my-receiver-1234",
##"auto-load": true,
##"parameters": [
##                  {
##                      "name": "data",
##                      "description": "file data",
##                      "required": true,
##                      "type": "DataFile",
##                      "state" : "PRE_PROC"
##                  }
## ],
##"return": [
##              {
##                      "name": "data",
##                      "description": "file data",
##                      "required": true,
##                      "type": "DataFile",
##                      "state" : "PROCESSED"
##                  }
##
##          ] }
##END-CONF




from pumpkin import *

class receiver(PmkSeed.Seed):

    def __init__(self, context, poi=None):
        PmkSeed.Seed.__init__(self, context,poi)
        self.counter = 0
        pass


    def run(self, pkt, data):

        if self.ifFile(data):
            prot, path, file, apath,_ = self.fileparts(data)

            f = open(apath, 'r')
            s = f.read()
            f.close()

            print "FILE DATA FROM: "+apath+": "+s

        else:

            print "NON FILE DATA: "+data




        pass

