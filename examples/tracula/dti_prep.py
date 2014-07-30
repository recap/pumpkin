__author__ = 'reggie'


###START-CONF
##{
##"object_name": "dti_prep",
##"object_poi": "dti_prep-0001",
##"auto-load": true,
##"parameters": [
##               {
##                      "name": "dti_input",
##                      "type": "Composite",
##                      "state" : "DTI_RAW"
##                }
##              ],
##"return": [
##              {
##                      "name": "dti_output",
##                      "type": "Composite",
##                      "state" : "DTI_PREPROC"
##                }
##
##          ] }
##END-CONF




from pumpkin import *

class dti_prep(PmkSeed.Seed):

    def __init__(self, context, poi=None):
        PmkSeed.Seed.__init__(self, context,poi)
        pass


    def run(self, pkt, data):

        self.logger.debug("In dti_prep data: "+str(data))



        pass

