__author__ = 'reggie'

###START-CONF
##{
##"object_name": "StubModel",
##"object_poi": "vph-101",
##"parameters": [
##                 {
##                      "name": "StubModelParam",
##                      "description": "data directory tarred",
##                      "required": true,
##                      "type": "StringFileTar",
##                      "format": "FileString",
##                      "state" : "XSimX1X2"
##                  }
##              ],
##"return": [
##              {
##                      "name": "Xsim",
##                      "description": "returns Xsim output",
##                      "required": true,
##                      "type": "StringFileTar",
##                      "format": "FileString",
##                      "state" : "XsimOut"
##                  }
##
##          ] }
##END-CONF





import subprocess
import os
import shutil
from pumpkin import PmkSeed


class StubModel(PmkSeed.Seed):

    def __init__(self, context, poi=None):
        PmkSeed.Seed.__init__(self, context,poi)
        self.wd = self.context.getWorkingDir()

        #Set environment
        #self.env = os.environ.copy()
        #self.env['R_LIBS'] = self.wd
        #self.script_path = self.wd+"/None"
        pass

    def on_load(self):
        print "Loading: " + self.__class__.__name__
        #shutil.copy(self.wd+"DataPacket.pkt", self.wd+"rx/DataPacket.pkt")
        pass


    def run(self, pkt, TarFile):

        prot,rel_path,filep,apath = self.fileparts(TarFile)

        new_file_name = self.get_ship_id(pkt)+"-"+self.get_cont_id(pkt)+"-"+self.get_name()
        self.logger.debug("Adding stub Xsim-ouput.csv to tar file ["+filep+"]")
        #fout = self._add_to_tar("/data/Xsim-output.csv", filep, postfix=self.get_cont_id(pkt), rename=new_file_name)
        fout = self._add_to_tar("/data/Xsim-output.csv", filep, rename=new_file_name)

        if fout:
            self.logger.debug("Dispatching file: "+str(fout))
            self.dispatch(pkt,"file://"+str(fout), "XsimOut")

        pass

