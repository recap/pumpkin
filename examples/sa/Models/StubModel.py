__author__ = 'reggie'

###START-CONF
##{
##"object_name": "StubModel",
##"object_poi": "vph-101",
##"group" : "public",
##"remoting" : "False",
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

    def split(self, pkt, data):
        """ Split the pkt into many packets to distribute processing.
            In this case we need to untar the X1 X2 Xsim csv files,
            grab a single entry from each file into a new file for X1' X2' Xsim'
            retar the files and dispatch.

            After splitting and dispatching packets. A termination packet needs to be sent with the number
            of packets as its payload like so:

            # lpkt = self.last_fragment_pkt(pkt, frag_no+1)
            # self.dispatch(lpkt, str(frag_no-1), "XSimX1X2", type="FileString")

            Where:
                lpkt is a termination packet
                frag_no is the number of split packets


            When implemented change the return to True else split will not be invoked.
        """

        return False


    def run(self, pkt, TarFile):

        prot,rel_path,filep,apath,rpath = self.fileparts(TarFile)

        new_file_name = self.get_ship_id(pkt)+"-"+self.get_cont_id(pkt)+"-"+self.get_name()
        self.logger.debug("Adding stub Xsim-ouput.csv to tar file ["+filep+"]")
        fout = self._add_to_tar("/data/Xsim-output.csv", rpath, rename=new_file_name)

        if fout:
            self.logger.debug("Dispatching file: "+str(fout))
            self.dispatch(pkt,"file://"+str(fout), "XsimOut")
        pass


    def merge(self, pkt, data):
        """ Merging does the opposite of splitting. Merge function is called
            after every run(). To make sure all split packets are received we
            can keep a counter as below. After all packets are received you can
            merge the data into a new packet.


        # if self.is_last_fragment(pkt):
        #     self.last_received = True
        #
        #     # Number of split packets expected
        #     self.exp_pkts = int(data)
        # else:
        #
        #     # Counter to keep track of split packets
        #     self.packt_co += 1
        #     self.str_pkts.append(pkt)
        #
        # # Merge when all packets received
        # if self.packt_co == self.exp_pkts:
        #     for spkt in self.str_pkts:
        #         pkt_data = self.get_pkt_data(spkt)
        #         # Do Merging Untat, combine, retar
        #
        #
        #     npkt = self.clean_header(pkt)
        #     self.dispatch(npkt,"file://"+filepath, "XsimOut")


        """
        pass