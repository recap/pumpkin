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
        self.logger.info("[skymodel] processing: " + str(input_folder))
	SkymodelCal = main(ms_input=str(input_folder), DirSkymodelCal=DirSkymodelCal, extensionSky=".skymodel",)["SkymodelCal"]
	self.logger.info("[skymodel] skymodelCal: {}".format(SkymodelCal))
	copyfile(path.join(DirSkymodelCal, SkymodelCal), str(input_folder) + "/selected.skymodel")
	
        self.dispatch(pkt, str(input_folder), "SKYMODEL")
        pass
