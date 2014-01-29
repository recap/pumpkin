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
##                      "type": "DataString",
##                      "state" : "PROCESSED"
##                  }
## ],
##"return": [
##              {
##                      "name": "data",
##                      "description": "file data",
##                      "required": true,
##                      "type": "DataString",
##                      "state" : "RECEIVED"
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
        # if self.counter < 2:
        #     self.counter += 1
        #     raise Exception('spam', 'eggs')

        if self.ifFile(data):
            prot,path,file,apath,_ = self.fileparts(data)

            f = open(apath, 'r')
            s = f.read()
            f.close()

            print "FILE DATA FROM: "+apath+": "+s

        else:

            print "R: "+data

        #time.sleep(10)

        #print str(json.dumps(pkt))

        #self.ack_pkt(pkt)


        pass

