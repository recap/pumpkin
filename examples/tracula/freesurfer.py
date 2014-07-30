__author__ = 'reggie'


###START-CONF
##{
##"object_name": "freesurfer",
##"object_poi": "freesurfer-0001",
##"auto-load": true,
##"parameters": [
##              {
##                      "name": "mri_input",
##                      "type": "Composite",
##                      "state" : "MRI_RAW"
##                }
## ],
##"return": [
##              {
##                      "name": "mri",
##                      "type": "Composite",
##                      "state" : "MRI_BRAINSEGMENT"
##                }
##
##          ] }
##END-CONF




from pumpkin import *

class freesurfer(PmkSeed.Seed):

    def __init__(self, context, poi=None):
        PmkSeed.Seed.__init__(self, context,poi)
        pass


    def run(self, pkt, data):
        self.logger.debug("In freesurfer data: "+str(data))



        pass

