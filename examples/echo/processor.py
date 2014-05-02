__author__ = 'reggie'


###START-CONF
##{
##"object_name": "processor",
##"object_poi": "my-processor-1234",
##"auto-load": true,
##"remoting" : true,
##"parameters": [
##                  {
##                      "name": "data",
##                      "description": "file data",
##                      "required": true,
##                      "type": "DataString",
##                      "state" : "RAW"
##                  }
## ],
##"return": [
##              {
##                      "name": "data",
##                      "description": "file data",
##                      "required": true,
##                      "type": "DataString",
##                      "state" : "PROCESSED"
##                  }
##
##          ] }
##END-CONF




from pumpkin import *

class processor(PmkSeed.Seed):

    def __init__(self, context, poi=None):
        PmkSeed.Seed.__init__(self, context,poi)
        self.str_pkts = {}
        self.packet_counter = 0;
        self.expected_pkts = 0
        self.last_received = False
        self.merged_data = ""
        pass



    def split(self, pkt, data):
        """
            The split() can be used to define how a packet is split. The usage scenarios are
            when needs to be partitioned during workflow execution. Where data is already
            pre-partitioned in for example in files then its best to separate data packets
            from the sender run().

            How to split the data in the packet is user specified. In this case we split on every character
            in the data string,

            self.fragment_pkt(pkt, frag_no) : returns a new fragmented packet. The id of the fragment is given through
            frag_no which can simply be an integer index.

            self.dispatch(npkt, a, "RAW", type="DataString") : The packets needs to be dispatched with the input parameter stag
            as the split is an intermediate function before run() thus the output of split is channeled through run()

            self.last_fragment_pkt(pkt, frag_no+1) : The last packet is specially marked packet where the data payload is just
            the number of fragments.
        """


        time.sleep(5)
        frag_no = 1
        for a in data:

            npkt = self.fragment_pkt(pkt, frag_no)
            frag_no += 1
            self.dispatch(npkt, a, "RAW", type="DataString")

        lpkt = self.last_fragment_pkt(pkt, frag_no+1)
        self.dispatch(lpkt, str(frag_no-1), "RAW", type="DataString")

        return True

    def run(self, pkt, data):
        """
        The run() will process a whole packet or a fragment. Distributed versions of the run function will process fragments
        in parallel. The first processor that calls split() will act as a master i.e. only the master will run split/merge and the replicas will
        run run().
        """
        print "PROCESSOR: "+data
        #self.ack_pkt(pkt)
        self.dispatch(pkt, data, "PROCESSED")
        pass

    def merge(self, pkt, data):
        """
        Merging is somewhat the reverse as split(). It is user defined also but a little more tricky this is because merging
        has to occur on some number of packets. If merging has to be done on all fragments then we need to wait for the last
        packet signal. The last packet contains the number of packets that should have been received but because of the out of order
        reception of packets the last packet can be received before the others. For this reason we have to confirm that all packets
        have been received by keeping a counter.

        This example stores intermediate packets in a hashtable with their fragment number as their key. Merging can then be done by
        iterating through the table. Since it is application dependant, the user if free to decide how to store fragments for example
        on disk etc.
        """

        print "MERGE FRAGMENT: "+data
        if self.is_last_fragment(pkt):
            self.last_received = True
            self.expected_pkts = int(data)
        else:
            self.packet_counter += 1
            fragment_no = self.get_pkt_fragment_no(pkt)
            self.str_pkts[fragment_no] = pkt

        if self.packet_counter == self.expected_pkts:
            for k in sorted(self.str_pkts.keys()):
                spkt = self.str_pkts[k]
                pkt_data = self.get_pkt_data(spkt)
                self.merged_data += pkt_data

            print "MERGE DATA: "+self.merged_data
            npkt = self.clean_header(pkt)
            self.dispatch(npkt, self.merged_data, "PROCESSED")

        pass

