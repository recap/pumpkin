__author__ = 'reggie'


###START-CONF
##{
##"object_name": "tracula",
##"object_poi": "tracula-1234",
##"auto-load": true,
##"parameters": [
##                  {
##                      "name": "tracula_input",
##                      "type": "Composite",
##                      "state" : "MRI_BRAINSEGMENT&DTI_PREPROC&DTI_FIBER"
##                  }
## ],
##"return": [
##              {
##                      "name": "tracula_ouput",
##                      "type": "Composite",
##                      "state" : "BRAIN_REGION_TRACK"
##               }
##
##          ] }
##END-CONF




from pumpkin import *

class tracula(PmkSeed.Seed):

    def __init__(self, context, poi=None):
        PmkSeed.Seed.__init__(self, context,poi)
        self.str_pkts = []
        self.packt_co = 0;
        self.exp_pkts = 0
        self.last_received = False
        self.merged_data = ""
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

        print "tracula: "+data
        #self.ack_pkt(pkt)
        self.dispatch(pkt, data, "REGIONTRACK")
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
    #         print "traculaERGE DATA: "+self.merged_data
    #         npkt = self.clean_header(pkt)
    #         self.dispatch(npkt, self.merged_data, "PROCESSED")
    #
    #     pass

