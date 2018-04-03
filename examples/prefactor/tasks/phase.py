__author__ = 'reggie'

###START-CONF
##{
##"object_name": "phase",
##"object_poi": "my-phase-1234",
##"auto-load" : true,
##"parameters": [ {
##                  "name": "name",
##                  "description": "",
##                  "required": true,
##                  "type": "String",
##                  "state" : "H5"
##              } ],
##"return": [
##              {
##                  "name": "phaseing",
##                  "description": "a phaseing",
##                  "required": true,
##                  "type": "String",
##                  "state" : "PHASE"
##               }
##
##          ] }
##END-CONF




from pumpkin import *

import tempfile
import datetime
import os
from find_cal_global_phaseoffset_losoto import main

class phase(PmkSeed.Seed):


    def __init__(self, context, poi=None):
        PmkSeed.Seed.__init__(self, context,poi)
        pass


    def run(self, pkt, name):
        self.logger.info('[phase] start')
	workingdir = tempfile.mkdtemp()
	losoto = str(name[0]) + '/losoto.h5'
	currentdir = os.getcwd()
	os.chdir(workingdir)
	main(losotoname=losoto, store_basename="cwl", refstationID=2, sourceID=0)
	os.chdir(currentdir)
	self.logger.info('[phase] output at ' + workingdir)
	self.logger.info('[phase] done.')
        self.dispatch(pkt, workingdir, "PHASE")
	pass
