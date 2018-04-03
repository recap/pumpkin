__author__ = 'reggie'

###START-CONF
##{
##"object_name": "output",
##"object_poi": "my-output-1234",
##"parameters": [ {
##                  "name": "output",
##                  "description": "a output",
##                  "required": true,
##                  "type": "String",
##                  "state" : "PLOT&PLOTCALPHASES&PHASE"
##              } ],
##"return": []
##}
##END-CONF




from pumpkin import *
from shutil import copytree
from shutil import rmtree

pfcache = {}
outputdir = '/output/'

class output(PmkSeed.Seed):

    def __init__(self, context, poi=None):
        PmkSeed.Seed.__init__(self, context,poi)
        pass


    def run(self, pkt, data):
        global pfcache
        print('[output] received: ' + str(data[0]) + ' with state ' + pkt[0]['t_state'])
        intermdir = str(data[0])
        state = str(pkt[0]['t_state'])
        pfcache[state] = intermdir
        rmtree(outputdir + state, ignore_errors=True)
        copytree(intermdir, outputdir + state)

        if 'PLOT' in pfcache and 'PHASE' in pfcache and 'PLOTCALPHASES' in pfcache:
            print('[output] all output written to ' + outputdir)
            print('[output] workflow done.')
            pfcache = {}
        pass
