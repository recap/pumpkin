__author__ = 'reggie'


###START-CONF
##{
##"object_name": "producer",
##"object_poi": "my-producer-1234",
##"auto-load": true,
##"parameters": [ ],
##"return": [
##              {
##                      "name": "data",
##                      "description": "file data",
##                      "required": true,
##                      "type": "DataFile",
##                      "state" : "PRE_PROC"
##                }
##
##          ] }
##END-CONF




from pumpkin import *

class producer(PmkSeed.Seed):

    def __init__(self, context, poi=None):
        PmkSeed.Seed.__init__(self, context,poi)
        pass


    def run(self, pkt):
        """ Write to a file and send it to a
            consumer.
        """
        wd = self.context.getWorkingDir()
        fs = self.context.getFileDir()
        fn = "testfile.txt"

        full_wfn = wd+"/"+fn

        f = open(full_wfn, 'w')
        f.write("some data")
        f.close()

        self.dispatch(pkt,"file://"+full_wfn, "PRE_PROC")


        pass

