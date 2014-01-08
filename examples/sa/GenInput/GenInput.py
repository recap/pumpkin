__author__ = 'reggie'

###START-CONF
##{
##"object_name": "GenInput",
##"object_poi": "vph-101",
##"parameters": [
##                 {
##                      "name": "GenInputParam",
##                      "description": "parameters file",
##                      "required": true,
##                      "type": "StringCSV",
##                      "format": "FileString,Integer",
##                      "state" : "SAInput"
##                  }
##              ],
##"return": [
##              {
##                      "name": "Xsim",
##                      "description": "returns tarball with X1 X2 and Xsim files",
##                      "required": true,
##                      "type": "StringFile",
##                      "format": "",
##                      "state" : "XSimX1X2"
##                  }
##
##          ] }
##END-CONF





import subprocess
import os
from pumpkin import PmkSeed


class GenInput(PmkSeed.Seed):

    def __init__(self, context, poi=None):
        PmkSeed.Seed.__init__(self, context,poi)
        self.env = os.environ.copy()
        self.env['LD_LIBRARY_PATH'] += ":/usr/local/matlabR2008a/bin/glnxa64/"
        self.script_path = "/home/reggie/PUMPKIN/examples/sa/GenInput/GenInput_generic"
        pass

    def on_load(self):
        print "Loading: " + self.__class__.__name__
        pass


    def run(self, pkt, GenInputParam):
        InputName = GenInputParam.split(",")[0]
        NrOfSamplesParam = GenInputParam.split(",")[1]

        ret = subprocess.call([self.script_path, InputName, NrOfSamplesParam],env=self.env, cwd=self.context.getWorkingDir())
        if ret == 0:
            #Success

            pass
        else:
            #Error
            pass


        #self.dispatch(pkt, self.is_a(tw), "ISA")

        pass

