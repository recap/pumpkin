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
        self.wd = self.context.getWorkingDir()
        self.env = os.environ.copy()
        self.env['LD_LIBRARY_PATH'] += ":/usr/local/matlabR2008a/bin/glnxa64/"
        self.script_path = self.wd+"/GenInput_generic"
        pass

    def on_load(self):
        print "Loading: " + self.__class__.__name__
        pass


    def run(self, pkt, GenInputParam):
        ParamFile = GenInputParam.split(",")[0]
        NrOfSamplesParam = GenInputParam.split(",")[1]
        InputFile = self.move_file_to_wd(ParamFile)

        print "RUNNING GenInput" + str(NrOfSamplesParam)
        ret = subprocess.call([self.script_path, InputFile, NrOfSamplesParam],env=self.env, cwd=self.context.getWorkingDir())
        #ret = subprocess.call([self.script_path, "testje3.txt", NrOfSamplesParam],env=self.env, cwd=self.context.getWorkingDir())
        if ret == 0:
            #Sucess
            dst_dir = "/out_data/"+self.get_ship_id(pkt)
            self.move_data_file("input-variation.txt", dst_dir, self.get_cont_id(pkt))
            self.move_data_file("X1-input.dat", dst_dir, self.get_cont_id(pkt))
            self.move_data_file("X2-input.dat", dst_dir, self.get_cont_id(pkt))
            self.move_data_file("Xsim-input.csv", dst_dir, self.get_cont_id(pkt))

            pass
        else:
            #Error
            pass


        #self.dispatch(pkt, self.is_a(tw), "ISA")

        pass

