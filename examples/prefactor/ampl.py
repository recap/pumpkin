__author__ = 'reggie'

###START-CONF
##{
##"object_name": "ampl",
##"object_poi": "my-ampl-1234",
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
##                  "name": "ampling",
##                  "description": "a ampling",
##                  "required": true,
##                  "type": "String",
##                  "state" : "AMPL"
##               }
##
##          ] }
##END-CONF




from pumpkin import *

import tempfile
import datetime
import os
from subprocess import Popen

class ampl(PmkSeed.Seed):


    def __init__(self, context, poi=None):
        PmkSeed.Seed.__init__(self, context,poi)
        pass


    def run(self, pkt, name):
        print('[amql] start')
	workingdir = tempfile.mkdtemp()
	losoto = str(name[0]) + '/losoto.h5'
	currentdir = os.getcwd()
	cmd = ['python',
            '/usr/lib/prefactor/scripts/amplitudes_losoto_3.py',
            losoto,
            'fitclock',
            '4']
	Popen(cmd, cwd=workingdir).communicate()
	print('[ampl] output at ' + workingdir)
	print('[ampl] done.')
        self.dispatch(pkt, workingdir, "AMPL")
	pass
