__author__ = 'reggie'

###START-CONF
##{
##"object_name": "busyworker",
##"object_poi": "my-busyworker-1234",
##"auto-load" : true,
##"parameters": [ {
##                  "name": "name",
##                  "description": "name to busyworker",
##                  "required": true,
##                  "type": "String",
##                  "state" : "BRAW"
##              } ],
##"return": [
##              {
##                  "name": "busyworkering",
##                  "description": "a busyworkering",
##                  "required": true,
##                  "type": "String",
##                  "state" : "NULL"
##               }
##
##          ] }
##END-CONF




from pumpkin import *

class busyworker(PmkSeed.Seed):

    def __init__(self, context, poi=None):
        PmkSeed.Seed.__init__(self, context,poi)
        pass


    def run(self, pkt, data):
        id = str(data[0])

        print "Busy Loop :" + id
        while True:
            pass


        pass
