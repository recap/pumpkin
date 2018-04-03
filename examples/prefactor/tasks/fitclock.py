__author__ = 'reggie'

###START-CONF
##{
##"object_name": "fitclock",
##"object_poi": "my-fitclock-1234",
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
##                  "name": "fitclocking",
##                  "description": "a fitclocking",
##                  "required": true,
##                  "type": "String",
##                  "state" : "FITCLOCK"
##               }
##
##          ] }
##END-CONF




from pumpkin import *

import tempfile
import datetime
import os
from subprocess import Popen

class fitclock(PmkSeed.Seed):


    def __init__(self, context, poi=None):
        PmkSeed.Seed.__init__(self, context,poi)
        pass


    def run(self, pkt, name):
        self.logger.info('[fitclock] start')
	workingdir = tempfile.mkdtemp()
	losoto = str(name[0]) + '/losoto.h5'
	currentdir = os.getcwd()
	cmd = ['python',
            '/usr/lib/prefactor/scripts/fit_clocktec_initialguess_losoto.py',
            losoto,
            'fitclock',
            '1']
	Popen(cmd, cwd=workingdir).communicate()
	self.logger.info('[fitclock] output at ' + workingdir)
	self.logger.info('[fitclock] done.')
        self.dispatch(pkt, workingdir, "FITCLOCK")
	pass
