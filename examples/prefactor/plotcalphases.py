__author__ = 'reggie'

###START-CONF
##{
##"object_name": "plotcalphases",
##"object_poi": "my-plotcalphases-1234",
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
##                  "name": "plotcalphasesing",
##                  "description": "a plotcalphasesing",
##                  "required": true,
##                  "type": "String",
##                  "state" : "H5IMP"
##               }
##
##          ] }
##END-CONF




from pumpkin import *

import tempfile
import datetime
import os
from subprocess import Popen

class plotcalphases(PmkSeed.Seed):


    def __init__(self, context, poi=None):
        PmkSeed.Seed.__init__(self, context,poi)
        pass


    def run(self, pkt, name):
#	--env=TMPDIR=/tmp \
 #   --env=HOME=/var/spool/cwl \
  #  kernsuite/prefactor \
  #  losoto \
  #  /var/spool/cwl/losoto.h5 \
  #  losoto.parset
	
	losoto_parset = """flags                        =  [hdf5file]
LoSoTo.Steps                 =  [plot]
LoSoTo.Solset                =  [sol000]
LoSoTo.Soltab                =  [sol000/phase000]
LoSoTo.SolType               =  [phase]
LoSoTo.ant                   =  []
LoSoTo.pol                   =  [XX,YY]
LoSoTo.dir                   =  [pointing]
LoSoTo.Steps.plot.Operation  =  PLOT
LoSoTo.Steps.plot.PlotType   =  2D
LoSoTo.Steps.plot.Axes       =  [time,freq]
LoSoTo.Steps.plot.TableAxis  =  [ant]
LoSoTo.Steps.plot.ColorAxis  =  [pol]
LoSoTo.Steps.plot.Reference  =  CS001HBA0 
LoSoTo.Steps.plot.PlotFlag   =  False
LoSoTo.Steps.plot.Prefix     =  cwl_"""

	workingdir = tempfile.mkdtemp()
	file = open(str(name[0]) + "/losoto.parset", "w")
	file.write(losoto_parset)
	file.close()

	losoto = str(name[0]) + '/losoto.h5'
	losoto_p = str(name[0]) + '/losoto.parset'
	currentdir = os.getcwd()
	cmd = ['losoto',
		losoto,
		losoto_p]
	Popen(cmd, cwd=workingdir).communicate()
	print('[plotcalphases] output at ' + workingdir)
	print('[plotcalphases] done.')
	pass
