###START-CONF
##{
##"object_name": "fastainject",
##"object_poi": "qpwo-2345",
##"auto-load": true,
##"remoting" : false,
##"parameters": [
##
##              ],
##"return": [
##              {
##                      "name": "fasta",
##                      "description": "raw fasta",
##                      "required": true,
##                      "type": "FastaString",
##                      "format": "",
##                      "state" : "RAW"
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

class fastainject(PmkSeed.Seed):

    def __init__(self, context, poi=None):
        PmkSeed.Seed.__init__(self, context,poi)

    def on_load(self):
        print "Loading: " + self.__class__.__name__



    def run(self, pkt):
        matrix = matlist.blosum62
        gap_open = -10
        gap_extend = -0.5
        dir = expanduser("~")+"/fasta/"
        onlyfiles = [ f for f in listdir(dir) if isfile(join(dir,f)) ]
        for fl in onlyfiles:
            fullpath = dir+fl
            if( fl[-5:] == "fasta"):
                print "File: "+str(fl)
                pp = SeqIO.parse(open(fullpath, "rU"), "fasta")
                first_record = pp.next()
                second_record = pp.next()
                print "First: "+first_record.seq
                print "Second: "+second_record.seq
                SeqIO.parse(open(fullpath, "rU"), "fasta")

                alns = pairwise2.align.globalds(first_record.seq, second_record.seq, matrix, gap_open, gap_extend)
                top_aln = alns[0]
                aln_human, aln_mouse, score, begin, end = top_aln

                print aln_human+'\n'+aln_mouse


