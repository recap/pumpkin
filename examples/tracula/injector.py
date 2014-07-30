__author__ = 'reggie'


###START-CONF
##{
##"object_name": "injector",
##"object_poi": "injector-9192",
##"auto-load": true,
##"remoting" : true,
##"parameters": [
##              {
##                      "name": "inject",
##                      "type": "Composite",
##                      "state" : "ALL_RAW",
##                      "format" : "MRIFile,DTIFile"
##                }
##   ],
##"return": [
##              {
##                      "name": "token",
##                      "type": "Composite",
##                      "state" : "MRI_RAW&DTI_RAW"
##                }
##
##          ] }
##END-CONF




from pumpkin import *
import os.path


class injector(PmkSeed.Seed):

    def __init__(self, context, poi=None):
        PmkSeed.Seed.__init__(self, context,poi)
        self.home = os.path.expanduser("~")
        pass


    def run(self, pkt, msg):

        data = msg.split(",")


        mri_file = self.home+"/"+data[0]
        dti_file = self.home+"/"+data[1]

        found = True

        if os.path.isfile(mri_file):
            self.logger.info("Found input file: "+mri_file)
        else:
            found = False
            self.logger.warn("Input file "+mri_file+" not found yet...retry later")

        if os.path.isfile(dti_file):
            self.logger.info("Found input file: "+dti_file)
        else:
            found = False
            self.logger.warn("Input file "+dti_file+" not found yet...retry later")

        self.dispatch(pkt, mri_file, "MRI_RAW")
        self.dispatch(pkt, dti_file, "DTI_RAW")

        # for x in range(1,3):
        #     npkt = self.duplicate_pkt_new_box(pkt)
        #     self.dispatch(npkt, "##############################TEST####### "+str(x), "MRI_RAW")


        pass

