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
from subprocess import call


'''
shell script call example:-

convertDTI.cvmfs.sh preDTI_input.zip xxx preDTI_output_bedpostx_input.zip 7.11 1.0.1

$1 = input file
$2 = output dir
$3 = output file
$4 = some version
$5 = another version
'''


class dti_prep(PmkSeed.Seed):

    def __init__(self, context, poi=None):
        PmkSeed.Seed.__init__(self, context,poi)
        self.home = os.path.expanduser("~")
        self.wd = self.context.getWorkingDir()
        self.script = "convertDTI.cvmfs.sh"
        self.dav_dir = self.home+"/traculadav/"
        pass


    def run(self, pkt, data):

        self.logger.debug("In dti_prep data: "+str(data))

        ship_id = self.get_ship_id(pkt)
        script_path = self.wd+self.copy_file_to_wd(self.dav_dir+self.script, 0755)
        dti_file = self.copy_file_to_wd(data[0])
        output_file = "output-"+self.get_name()+"-"+ship_id+".zip"

        call([script_path, dti_file,"xxx",output_file, "7.11", "1.0.1"], cwd=self.context.getWorkingDir())


        dav_wd = self.dav_dir+ship_id
        self._ensure_dir(dav_wd)
        shutil.move(self.wd+"/"+output_file,dav_wd+"/"+output_file)

        message = dav_wd+"/"+output_file
        self.dispatch(pkt, message, "DTI_PREPROC")




        pass

