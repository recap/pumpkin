__author__ = 'reggie'

###START-CONF
##{
##"object_name": "GenInput",
##"object_poi": "vph-101",
##"group" : "public",
##"remoting" : "False",
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
##                      "type": "StringFileTar",
##                      "format": "",
##                      "state" : "XSimX1X2"
##                  }
##
##          ] }
##END-CONF



import subprocess
import os,shutil
from pumpkin import PmkSeed


class GenInput(PmkSeed.Seed):

    def __init__(self, context, poi=None):
        PmkSeed.Seed.__init__(self, context,poi)
        self.wd = self.context.getWorkingDir()
        self.env = os.environ.copy()
        self.env['LD_LIBRARY_PATH'] += ":/usr/local/matlabR2008a/bin/glnxa64/"
        #VPH
        #self.env['LD_LIBRARY_PATH'] = "/opt/MATLAB_Compiler_Runtime/v78/runtime/glnxa64"
        self.script_path = self.wd+"/GenInput_generic"


        pass

    def on_load(self):
        self.logger.info("Loading: " + self.__class__.__name__)

        shutil.copy(self.wd+"example_packets/DataPacket-GenInput.pkt", self.wd+"rx/DataPacket.pkt")
        pass


    def run(self, pkt, GenInputParam):
        ParamFile = GenInputParam.split(",")[0]
        NrOfSamplesParam = GenInputParam.split(",")[1]
        #InputFile = "./"+self.move_file_to_wd(ParamFile)
        InputFile = self.get_relative_path(ParamFile)

        self.logger.info("Executing shell script: "+self.script_path)
        ret = subprocess.call([self.script_path, InputFile, NrOfSamplesParam],env=self.env, cwd=self.context.getWorkingDir())
        self.logger.info("Shell script return: "+str(ret))
        if ret == 0:

            dst_dir = "/out_data/"+self.get_ship_id(pkt)
            # self.move_data_file("input-variation.txt", dst_dir, self.get_cont_id(pkt))
            # self.move_data_file("X1-input.dat", dst_dir, self.get_cont_id(pkt))
            # self.move_data_file("X2-input.dat", dst_dir, self.get_cont_id(pkt))
            # self.move_data_file("Xsim-input.csv", dst_dir, self.get_cont_id(pkt))

            self.move_data_file("input-variation.txt", dst_dir)
            self.move_data_file("X1-input.dat", dst_dir)
            self.move_data_file("X2-input.dat", dst_dir)
            self.move_data_file("Xsim-input.csv", dst_dir)

            output_file = "file://"+str(self._tar_to_gz(dst_dir,suffix=self.get_cont_id(pkt)+"-"+self.get_name()))
            #output_file = "file://"+self._tar_to_gz(dst_dir, destination="/out_data/", suffix=self.get_cont_id(pkt)+"-"+self.get_name())
            #output_file = "file://"+self._tar_to_gz(dst_dir, suffix=self.get_cont_id(pkt)+"-"+self.get_name())

            self.dispatch(pkt,output_file,"XSimX1X2")



            pass
        else:
            #Error
            pass


        #self.dispatch(pkt, self.is_a(tw), "ISA")

        pass

