__author__ = 'reggie'

###START-CONF
##{
##"object_name": "calibcalc",
##"object_poi": "my-calibcalc-1234",
##"auto-load" : true,
##"parameters": [ {
##                  "name": "skymodel",
##                  "description": "",
##                  "required": true,
##                  "type": "String",
##                  "state" : "SKYMODEL"
##              } ],
##"return": [
##              {
##                  "name": "calibcalcing",
##                  "description": "a calibcalcing",
##                  "required": true,
##                  "type": "String",
##                  "state" : "CALIBRATED"
##               }
##
##          ] }
##END-CONF




from pumpkin import *

from subprocess import Popen

class calibcalc(PmkSeed.Seed):

    def __init__(self, context, poi=None):
        PmkSeed.Seed.__init__(self, context,poi)
        pass


    def run(self, pkt, name):
	print("[calibcalc] processing: " + str(name[0]))
	input_folder = name[0]
	skymodel = input_folder + '/selected.skymodel'
	cmd = ["/usr/bin/calibrate-stand-alone",
		"--numthreads",
		"1",
    		input_folder,
    		"/usr/share/prefactor/parsets/calibcal.parset",
    		skymodel]

	Popen(cmd, env={"TMPDIR":"/tmp", "HOME":input_folder, "LOFARROOT":"/usr"}).communicate()

	print("[calibcalc] done: " + str(name[0]))
        self.dispatch(pkt, input_folder, "CALIBRATED")
        pass
