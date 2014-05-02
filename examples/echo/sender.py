__author__ = 'reggie'


###START-CONF
##{
##"object_name": "sender",
##"object_poi": "my-sender-1234",
##"auto-load": true,
##"parameters": [ ],
##"return": [
##              {
##                      "name": "data",
##                      "description": "string data",
##                      "required": true,
##                      "type": "DataString",
##                      "state" : "RAW"
##                }
##
##          ] }
##END-CONF




from pumpkin import *

class sender(PmkSeed.Seed):

    def __init__(self, context, poi=None):
        PmkSeed.Seed.__init__(self, context,poi)
        pass


    def run(self, pkt):
        """ Write to a file and send it to a
            consumer.
        """
        # wd = self.context.getWorkingDir()
        # fs = self.context.getFileDir()
        # fn = "testfile.txt"
        #
        # full_wfn = wd+"/"+fn
        #
        #
        # f = open(full_wfn, 'w')
        # f.write("some data")
        # f.close()
        #time.sleep(45)

        #self.dispatch(pkt, "The quick brown fox jumps over the lazy dog", "RAW")
        self.dispatch(pkt, "ABCDEFGHIJK","RAW")

        #npkt = self.duplicate_pkt_new_box(pkt)
        # for x in range(1,3):
        #     npkt1 = self.duplicate_pkt_new_box(pkt)
        #
        #     #print "SENDER: " +str(x)
        #
        #     self.dispatch(npkt1, "The quick brown fox jumps over the lazy dog", "RAW")
        #     #self.dispatch(npkt1, "##############################TEST####### "+str(x), "RAW")


        #npkt2 = self.duplicate_pkt_new_box(pkt)
        # npkt3 = self.duplicate_pkt_new_box(pkt)
        # npkt4 = self.duplicate_pkt_new_box(pkt)
        # npkt5 = self.duplicate_pkt_new_box(pkt)

        #self.dispatch(pkt,"file://"+full_wfn, "RAW")
        #self.dispatch(npkt, "test", "RAW")
        #self.dispatch(npkt1, "test1", "RAW")
        #self.dispatch(npkt2, "test2", "RAW")
        # self.dispatch(npkt3, "test3", "RAW")
        # self.dispatch(npkt4, "test4", "RAW")
        # self.dispatch(npkt5, "test5", "RAW")
        pass

