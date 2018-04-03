__author__ = 'reggie'


###START-CONF
##{
##"object_name": "start",
##"object_poi": "my-start-1234",
##"auto-load": true,
##"parameters": [ ],
##"return": [
##              {
##                      "name": "subbanddir",
##                      "description": "my name",
##                      "required": true,
##                      "type": "String",
##                      "state" : "SUBBAND"
##                  }
##
##          ] }
##END-CONF




from pumpkin import *

from os import listdir
from os.path import isfile, join
import datetime

class start(PmkSeed.Seed):

    def __init__(self, context, poi=None):
        PmkSeed.Seed.__init__(self, context,poi)
        pass


    def run(self, pkt):
	self.logger.info("[start] " + str(datetime.datetime.now()))
	onlydirs = [f for f in listdir('/data') if not isfile(join('/data/', f))]
	for d in onlydirs:
		sample_id = d[:7]
		self.fork_dispatch(pkt, '|,|'.join([d, sample_id]), "SUBBAND")
        pass

