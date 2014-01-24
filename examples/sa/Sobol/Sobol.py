__author__ = 'reggie'

###START-CONF
##{
##"object_name": "Sobol",
##"object_poi": "vph-101",
##"parameters": [
##                 {
##                      "name": "Sobol",
##                      "description": "data directory tarred",
##                      "required": true,
##                      "type": "StringTAR",
##                      "format": "FileString",
##                      "state" : "SobolInput"
##                  }
##              ],
##"return": [
##              {
##                      "name": "Sobol-Indeces",
##                      "description": "returns sobol indeces file",
##                      "required": true,
##                      "type": "StringFile",
##                      "format": "",
##                      "state" : "SobolIndeces"
##                  }
##
##          ] }
##END-CONF





import subprocess
import os
from pumpkin import PmkSeed


class Sobol(PmkSeed.Seed):

    def __init__(self, context, poi=None):
        PmkSeed.Seed.__init__(self, context,poi)
        self.wd = self.context.getWorkingDir()
        self.env = os.environ.copy()
        self.env['R_LIBS'] = self.wd
        self.script_path = self.wd+"/DFL_commands.sh"
        pass

    def on_load(self):
        print "Loading: " + self.__class__.__name__
        pass


    def run(self, pkt, TarFile):
        #InputName = GenInputParam.split(",")[0]
        #NrOfSamplesParam = GenInputParam.split(",")[1]

        self._untar_to_wd(TarFile)

        print "STARTING SOBOL"
        ret = subprocess.call([self.script_path],env=self.env, cwd=self.context.getWorkingDir())


        #ret = subprocess.call([self.script_path],env=self.env, cwd="/tmp/")
        if ret == 0:
            print "SOBOL OK"
            dst = self._tar_to_gz("/SimRes", suffix=self.get_cont_id(pkt))
            self.ack_pkt(pkt)
            self.dispatch(pkt, "file://"+dst, "SobolIndeces")

            pass
        else:
            print "SOBOL ERROR"
            pass


        #self.dispatch(pkt, self.is_a(tw), "ISA")

        pass

