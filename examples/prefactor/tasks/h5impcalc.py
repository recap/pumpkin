__author__ = 'reggie'

###START-CONF
##{
##"object_name": "h5impcalc",
##"object_poi": "my-h5impcalc-1234",
##"auto-load" : true,
##"parameters": [ {
##                  "name": "name",
##                  "description": "",
##                  "required": true,
##                  "type": "String",
##                  "state" : "CALIBRATED"
##              } ],
##"return": [
##              {
##                  "name": "h5impcalcing",
##                  "description": "a h5impcalcing",
##                  "required": true,
##                  "type": "String",
##                  "state" : "H5"
##               }
##
##          ] }
##END-CONF




from pumpkin import *

from subprocess import Popen
import tempfile
import datetime

no_of_bands = 20
h5_count = 0
input_dirs = {}

class h5impcalc(PmkSeed.Seed):


    def __init__(self, context, poi=None):
        PmkSeed.Seed.__init__(self, context,poi)
        pass


    def run(self, pkt, name):
	global h5_count
	global input_dirs
	input_dir = str(name[0])
        self.logger.info("[h5impcalc] received: " + input_dir)
	if not input_dir in input_dirs:
		h5_count += 1
		input_dirs[input_dir] = 1
	
	if h5_count == no_of_bands:
		self.logger.info("[h5impcalc] received all inputs")
		workingdir = tempfile.mkdtemp()

		cmd = ['python',
    			'/usr/lib/prefactor/scripts/losotoImporter.py',
    			'losoto.h5',
    			'-c',
    			'7',
    			'-s',
    			'sol000'] + list(input_dirs.keys())
		Popen(cmd, cwd=workingdir, env={'TMPDIR': '/tmp', 'HOME': workingdir}).communicate()
		self.logger.info("[h5impcalc] output in: " + workingdir)
		self.logger.info("[h5impcalc] done " + str(datetime.datetime.now()));

        	self.dispatch(pkt, workingdir, "H5")
		h5_count = 0
		input_dirs = {}
	pass
