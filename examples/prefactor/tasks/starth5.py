__author__ = 'reggie'


###START-CONF
##{
##"object_name": "starth5",
##"object_poi": "my-starth5-1234",
##"auto-load": false,
##"parameters": [ ],
##"return": [
##              {
##                      "name": "subbanddir",
##                      "description": "my name",
##                      "required": true,
##                      "type": "String",
##                      "state" : "H5"
##                  }
##
##          ] }
##END-CONF




from pumpkin import *

from os import listdir
from os.path import isfile, join
import datetime

class starth5(PmkSeed.Seed):

    def __init__(self, context, poi=None):
        PmkSeed.Seed.__init__(self, context,poi)
        pass


    def run(self, pkt):
	self.logger.info("[starth5] " + str(datetime.datetime.now()))
	d = '/tmp/tmpFFEvzt'
        self.dispatch(pkt, d, 'H5')
        pass

