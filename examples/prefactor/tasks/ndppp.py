__author__ = 'reggie'

###START-CONF
##{
##"object_name": "ndppp",
##"object_poi": "my-ndppp-1234",
##"auto-load" : true,
##"parameters": [ {
##                  "name": "name",
##                  "description": "name to greet",
##                  "required": true,
##                  "type": "String",
##                  "state" : "SUBBAND"
##              } ],
##"return": [
##              {
##                  "name": "calibrated",
##                  "description": "",
##                  "required": true,
##                  "type": "String",
##                  "state" : "PRECALIBRATED"
##               }
##
##          ] }
##END-CONF




from pumpkin import *

from subprocess import call

class ndppp(PmkSeed.Seed):

    def __init__(self, context, poi=None):
        PmkSeed.Seed.__init__(self, context,poi)
        pass


    def run(self, pkt, name):
	output = '/tmp/calibrated_' + str(name[0])
	cmd = ['NDPPP',
		'msout=' + str(output),
		'average.freqresolution=48.82kHz',
		'avg.freqstep=2',
		'average.timeresolution=4',
		'avg.timestep=2',
		'avg.type=average',
    		'baseline=[CS013HBA*]',
    		'filter.baseline=CS*', 
		'RS*&&',
		'filter.remove=True',
		'filter.type=filter',
		'flag.baseline=[ CS013HBA* ]',
    		'flag.type=filter',
    		'flagamp.amplmin=1e-30',
    		'flagamp.type=preflagger',
    		'msin=/data/' + str(name[0]),
    		'msin.datacolumn=DATA',
    		'msout.overwrite=True',
    		'msout.writefullresflag=False',
    		'steps=[flag,filter,avg,flagamp]']

	self.logger.info("[ndppp] calibrated: " + str(name[0]))
	call(cmd) 
        self.dispatch(pkt, output, "PRECALIBRATED")
        pass
