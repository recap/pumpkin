__author__ = 'reggie'

import time

class Endpoint(object):

    NEW_STATE = 1
    DISABLES_STATE = 2
    OK_STATE = 3

    TRACER_BURST = 10
    TRACER_INTERVAL = 100


    ### c_pred here ####

    @staticmethod
    def ready(ep, pkt):
        if "c_pred" in ep.keys():
            #p chooses ep priority from _get_priority_eps, setting it to 0 forces rescan
            p = 0
            t1 = time.time()
            t2 = ep["timestamp"]
            et = t1 - t2

            bklog = ep["wshift"]
            bklog -= et
            if bklog > 0:
                ep["locked"] = True
                return False
                #logging.debug("BACKLOG: "+str(bklog))
                #continue
            else:
                ep["locked"] = False
                ep["wshift"] = 0


            w = ep["wait"]

            w -= et
            if w < 0:
                #w = 0
                pred = ep["c_pred"]
                m = pred[0] # m in y = mx + c
                c = pred[1] # c in y = mx + c
                b = pred[2] #total queue backlog
                x = pkt[0]["c_size"]
                y = m*x + c
                #print "Adding: "+str(y)
                #adding
                ep["wait"] = (y) #+ b
                #logging.debug("WAIT: "+str(y)+" WSHIFT: "+str(b)+" X: "+str(x)+" M: "+str(m)+" C: "+str(c))
                #print "SETTING: "+str(b)
                ep["wshift"] = b
                pred[2] = 0
                ep["timestamp"] = t1
                return True

            else:
                #logging.debug("Waiting: "+str(w))
                return False
        else:
            return True