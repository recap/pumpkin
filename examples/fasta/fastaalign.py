###START-CONF
##{
##"object_name": "fastaalign",
##"object_poi": "qpwo-2345",
##"auto-load": true,
##"remoting" : false,
##"parameters": [
##              {
##                      "name": "fasta",
##                      "description": "raw fasta",
##                      "required": true,
##                      "type": "FastaString",
##                      "format": "",
##                      "state" : "RAW"
##                  }
##
##              ],
##"return": [
##              {
##                      "name": "fasta",
##                      "description": "raw fasta",
##                      "required": true,
##                      "type": "FastaString",
##                      "format": "",
##                      "state" : "ALLIGNED|NOTALLIGNED"
##                  }
##
##          ] }
##END-CONF



from os import listdir
from os.path import isfile, join
import pika
from os.path import expanduser
from Bio import SeqIO
from Bio import pairwise2
from Bio.SubsMat import MatrixInfo as matlist


from pumpkin import *

class fastaalign(PmkSeed.Seed):

    def __init__(self, context, poi=None):
        PmkSeed.Seed.__init__(self, context,poi)

    def on_load(self):
        print "Loading: " + self.__class__.__name__



    def run(self, pkt, data):
        matrix = matlist.blosum62
        gap_open = -10
        gap_extend = -0.5


        alns = pairwise2.align.globalds(data[0], data[1], matrix, gap_open, gap_extend)
        top_aln = alns[0]
        aln_human, aln_mouse, score, begin, end = top_aln

        #print aln_human+'\n'+aln_mouse


