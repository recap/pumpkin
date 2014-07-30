__author__ = 'reggie'


###START-CONF
##{
##"object_name": "freeview",
##"object_poi": "freeview-0001",
##"auto-load": true,
##"parameters": [
##                  {
##                      "name": "freeview_input",
##                      "type": "Composite",
##                      "state" : "BRAIN_REGION_TRACK"
##                  }
## ],
##"return": [
##                 {
##                      "name": "freeview_output",
##                      "type": "Visual",
##                      "state" : "BRAIN_VISUAL"
##                  }
##
##          ] }
##END-CONF




from pumpkin import *

class freeview(PmkSeed.Seed):

    def __init__(self, context, poi=None):
        PmkSeed.Seed.__init__(self, context,poi)
        pass


    def run(self, pkt, data):


        pass

