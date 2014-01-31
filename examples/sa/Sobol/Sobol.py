__author__ = 'reggie'

###START-CONF
##{
##"object_name": "Sobol",
##"object_poi": "vph-101",
##"group" : "public",
##"parameters": [
##                 {
##                      "name": "Sobol",
##                      "description": "data directory tarred",
##                      "required": true,
##                      "type": "StringFileTar",
##                      "format": "FileString",
##                      "state" : "XsimOut"
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

        self.logger.debug("Untaring: "+TarFile)
        prot,rel_path,filep,apath,rpath = self.fileparts(TarFile)

        self._untar_to_wd(rpath, destination=self.wd, rename="SimRes")

        self.logger.info("Executing shell script: "+self.script_path)
        ret = subprocess.call([self.script_path],env=self.env, cwd=self.context.getWorkingDir())
        self.logger.info("Shell script return: "+str(ret))
        dst = self._tar_to_gz("/SimRes", destination="/out_data/"+self.get_ship_id(pkt)+"/", suffix=self.get_cont_id(pkt))
        self.ack_pkt(pkt)
        self.dispatch(pkt, "file://"+dst, "SobolIndeces")

        #
        #
        # #ret = subprocess.call([self.script_path],env=self.env, cwd="/tmp/")
        # if ret == 0:
        #     print "SOBOL OK"
        #     dst = self._tar_to_gz("/SimRes", suffix=self.get_cont_id(pkt))
        #     self.ack_pkt(pkt)
        #     self.dispatch(pkt, "file://"+dst, "SobolIndeces")
        #
        #     pass
        # else:
        #     print "SOBOL ERROR"
        #     pass


        #self.dispatch(pkt, self.is_a(tw), "ISA")

        pass

