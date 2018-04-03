__author__ = 'reggie'

###START-CONF
##{
##"object_name": "skycal",
##"object_poi": "my-skycal-1234",
##"auto-load" : true,
##"parameters": [ {
##                  "name": "calibrated",
##                  "description": "",
##                  "required": true,
##                  "type": "String",
##                  "state" : "PRECALIBRATED"
##              } ],
##"return": [
##              {
##                  "name": "skycaling",
##                  "description": "a skycaling",
##                  "required": true,
##                  "type": "String",
##                  "state" : "SKYMODEL"
##               }
##
##          ] }
##END-CONF




from pumpkin import *

from find_skymodel_cal import main
from shutil import copyfile
from os import path
DirSkymodelCal="/usr/share/prefactor/skymodels/"



class skycal(PmkSeed.Seed):

    def __init__(self, context, poi=None):
        PmkSeed.Seed.__init__(self, context,poi)
        pass


    def run(self, pkt, calibrated_data):
	input_folder = calibrated_data[0]
        print("Processing: " + str(input_folder))
	SkymodelCal = main(ms_input=input_folder, DirSkymodelCal=DirSkymodelCal, extensionSky=".skymodel",)["SkymodelCal"]
	print("SkymodelCal: {}".format(SkymodelCal))
	copyfile(path.join(DirSkymodelCal, SkymodelCal), str(input_folder) + "/selected.skymodel")
	
        self.dispatch(pkt, input_folder, "SKYMODEL")
        pass
