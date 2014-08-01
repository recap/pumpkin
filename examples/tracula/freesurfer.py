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

class freesurfer(PmkSeed.Seed):

    def __init__(self, context, poi=None):
        PmkSeed.Seed.__init__(self, context,poi)
        self.home = os.path.expanduser("~")
        self.wd = self.context.getWorkingDir()
        self.script_path = self.wd+"/call_freesurfer.sh"
        pass


    def run(self, pkt, data):
        #data = msg.split("|,|")


        mri_file = self.copy_file_to_wd(self.home+"/"+data[0])
        subjectID = self.copy_file_to_wd(self.home+"/"+data[1])
        outputDir = self.copy_file_to_wd(self.home+"/"+data[2])
        lic = self.copy_file_to_wd(self.home+"/"+data[3])

        self.logger.info("Calling call_freesurfer.sh with: "+str(msg))
        #call([self.script_path, subjectID, outputDir, mri_file, "output.zip", "5.3.0"])



        pass

