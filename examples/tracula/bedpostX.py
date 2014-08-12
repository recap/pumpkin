__author__ = 'reggie'


###START-CONF
##{
##"object_name": "bedpostX",
##"object_poi": "bedpostX-0001",
##"auto-load": true,
##"parameters": [
##                  {
##                      "name": "bedpostx_input",
##                      "type": "Composite",
##                      "state" : "DTI_PREPROC"
##                  }
## ],
##"return": [
##              {
##                      "name": "bedpostx_output",
##                      "type": "Composite",
##                      "state" : "DTI_FIBER"
##               }
##
##          ] }
##END-CONF



from subprocess import call
from pumpkin import *

'''
example script call:-
bedpostx.sh predti-output.zip

$1 = input file
'''


class bedpostX(PmkSeed.Seed):

    def __init__(self, context, poi=None):
        PmkSeed.Seed.__init__(self, context,poi)

        self.home = os.path.expanduser("~")+"/"
        self.wd = self.context.getWorkingDir()
        self.script = "bedpostx.sh"
        self.dav_rel = "/traculadav/"
        self.dav_dir = self.home+self.dav_rel
        pass



    # def split(self, pkt, data):
    #
    #     frag_no = 1
    #     for a in data:
    #         npkt = self.fragment_pkt(pkt, frag_no)
    #         frag_no += 1
    #         #tag = "RAW:"+str(frag_no)
    #         self.dispatch(npkt, a, "RAW", type="DataString")
    #         #print a
    #     lpkt = self.last_fragment_pkt(pkt, frag_no+1)
    #     self.dispatch(lpkt, str(frag_no-1), "RAW", type="DataString")
    #
    #     return True

    def run(self, pkt, data):

        ship_id = self.get_ship_id(pkt)
        script_path = self.wd+self.copy_file_to_wd(self.dav_dir+self.script, 0755)
        predti_file = self.copy_file_to_wd(self.home+data[0])
        output_file = "result.tgz"

        call([script_path, predti_file], cwd=self.context.getWorkingDir())


        dav_wd = self.dav_dir+ship_id
        dav_re = self.dav_rel+ship_id
        self._ensure_dir(dav_wd)
        shutil.move(self.wd+"/"+output_file,dav_wd+"/"+output_file)

        message = dav_re+"/"+output_file

        self.dispatch(pkt, message, "DTI_FIBER")
        pass

    # def merge(self, pkt, data):
    #     if self.is_last_fragment(pkt):
    #         self.last_received = True
    #         self.exp_pkts = int(data)
    #     else:
    #         self.packt_co += 1
    #         self.str_pkts.append(pkt)
    #
    #     if self.packt_co == self.exp_pkts:
    #         for spkt in self.str_pkts:
    #             pkt_data = self.get_pkt_data(spkt)
    #             self.merged_data += pkt_data
    #
    #         print "bedpostXERGE DATA: "+self.merged_data
    #         npkt = self.clean_header(pkt)
    #         self.dispatch(npkt, self.merged_data, "PROCESSED")
    #
    #     pass

