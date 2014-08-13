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



from subprocess import call

from pumpkin import *

import os, sys, stat

'''
shell script call example:-
FS_CVMFS.sh subjectID outputDir freesurfer-sample-input.zip output.zip 5.3.0
'''

class freesurfer(PmkSeed.Seed):
    def __init__(self, context, poi=None):
        PmkSeed.Seed.__init__(self, context, poi)
        self.home = os.path.expanduser("~")+"/"
        self.wd = self.context.getWorkingDir()
        self.script = "FS_CVMFS.sh"
        self.dav_rel = "/traculadav/"
        self.dav_dir = self.home+self.dav_rel

        #self.script_path = self.wd + "/"+self.script

        pass


    def run(self, pkt, data):

        #data = msg.split("|,|")
        ship_id = self.get_ship_id(pkt)
        #self.logger.debug("FREESURFER: " + str(data))

        script_path = self.wd+self.copy_file_to_wd(self.dav_dir+self.script, 0755)
        #os.chmod(script_path, 0755)

        mri_file = self.copy_file_to_wd(self.home+data[0])
        subjectID = self.copy_file_to_wd(self.home+data[1])
        outputDir = self.copy_file_to_wd(self.home+data[2])
        lic = self.copy_file_to_wd(self.home+data[3])
        output_file = "output-"+self.get_name()+"-"+ship_id+".zip"

        self.logger.info("Calling FS_CVMFS.sh with: " + str(data))
        call([script_path, subjectID, outputDir, mri_file, output_file, "5.3.0"], cwd=self.context.getWorkingDir())

        dav_wd = self.dav_dir+ship_id
        dav_re = self.dav_rel+ship_id
        self._ensure_dir(dav_wd)
        shutil.move(self.wd+"/"+output_file,dav_wd+"/"+output_file)

        message = dav_re+"/"+output_file
        self.dispatch(pkt, message, "MRI_BRAINSEGMENT")



    pass

