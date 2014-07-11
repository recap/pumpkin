__author__ = 'reggie'

###START-CONF
##{
##"object_name": "worker",
##"object_poi": "my-worker-1234",
##"auto-load" : true,
##"parameters": [ {
##                  "name": "name",
##                  "description": "name to worker",
##                  "required": true,
##                  "type": "String",
##                  "state" : "RAW"
##              } ],
##"return": [
##              {
##                  "name": "workering",
##                  "description": "a workering",
##                  "required": true,
##                  "type": "String",
##                  "state" : "NULL"
##               }
##
##          ] }
##END-CONF




from pumpkin import *

class worker(PmkSeed.Seed):

    def __init__(self, context, poi=None):
        PmkSeed.Seed.__init__(self, context,poi)
        pass


    def run(self, pkt, sleep):
        isleep = int(sleep[0])
        print "Sleeping for :" + str(sleep[0])
        time.sleep(isleep)
        print "Woke up"


        pass
