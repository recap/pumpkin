__author__ = 'reggie'


###START-CONF
##{
##"object_name": "start",
##"object_poi": "my-start-1234",
##"auto-load": false,
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
	print("[start] " + str(datetime.datetime.now()))
	onlydirs = [f for f in listdir('/data') if not isfile(join('/data/', f))]
	for d in onlydirs:
		sample_id = d[:7]
		self.fork_dispatch(pkt, '|,|'.join([d, sample_id]), "SUBBAND")

	#print(onlydirs)
	#L570745
        #self.dispatch(pkt, "World", "UNGREETED")
        #self.fork_dispatch(pkt, "Mars", "UNGREETED")
        #self.fork_dispatch(pkt, "Venus", "UNGREETED")
        #self.fork_dispatch(pkt, "Jupiter", "UNGREETED")
        #self.fork_dispatch(pkt, "Mercury", "UNGREETED")
        pass

