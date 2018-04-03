__author__ = 'reggie'

###START-CONF
##{
##"object_name": "plot",
##"object_poi": "my-plot-1234",
##"auto-load" : true,
##"parameters": [ {
##                  "name": "name",
##                  "description": "",
##                  "required": true,
##                  "type": "String",
##                  "state" : "AMPL&FITCLOCK"
##              } ],
##"return": [
##              {
##                  "name": "ploting",
##                  "description": "a ploting",
##                  "required": true,
##                  "type": "String",
##                  "state" : "PLOT"
##               }
##
##          ] }
##END-CONF




from pumpkin import *

import tempfile
import datetime
import os

import matplotlib as mpl
mpl.use("Agg")
import numpy as np
import pylab


cache = {}

class plot(PmkSeed.Seed):


    def __init__(self, context, poi=None):
        PmkSeed.Seed.__init__(self, context,poi)
        pass


    def run(self, pkt, name):
        global cache
        print('[plot] received: ' + str(name[0] + " with state " + str(pkt[0]['t_state'])))
        cache[pkt[0]['t_state']] = str(name[0])

        if 'AMPL' in cache and 'FITCLOCK' in cache:
            print('[plot] received all data')
            ampldir = cache['AMPL']
            fitclockdir = cache['FITCLOCK']
            workingdir = tempfile.mkdtemp()
            currentdir = os.getcwd()
            os.chdir(workingdir)

            amparray   = np.load(ampldir + "/fitclock_amplitude_array.npy")
            clockarray = np.load(fitclockdir + "/fitted_data_dclock_fitclock_1st.sm.npy")
            dtecarray  = np.load(fitclockdir + "/fitted_data_dTEC_fitclock_1st.sm.npy")
            numants = len(dtecarray[0,:])

            for i in range(0,numants):
                pylab.plot(dtecarray[:,i])
            pylab.xlabel("Time")
            pylab.ylabel("dTEC [$10^{16}$ m$^{-2}$]")
            pylab.savefig("dtec_allsols.png")
            pylab.close()
            pylab.cla()

            for i in range(0,numants):
                pylab.plot(1e9*clockarray[:,i])
            pylab.xlabel("Time")
            pylab.ylabel("dClock [ns]")
            pylab.savefig("dclock_allsols.png")
            pylab.close()
            pylab.cla()


            for i in range(0,numants):
              pylab.plot(np.median(amparray[i,:,:,0], axis=0))
              pylab.plot(np.median(amparray[i,:,:,1], axis=0))
            pylab.xlabel("Subband number")
            pylab.ylabel("Amplitude")
            pylab.ylim(0,2.*np.median(amparray))
            pylab.savefig("amp_allsols.png")
            pylab.close()
            pylab.cla()


            os.chdir(currentdir)
            cache = {}
            print('[plot] output at ' + workingdir)
            print('[plot] done.')
            self.dispatch(pkt, workingdir, "PLOT")
	pass
