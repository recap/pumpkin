import os, time
import pumpkin
from pumpkin import PmkSeed


pmk = pumpkin.initialize_pumpkin(cli=False)

pmk.context.load_seed("mars.py")
pmk.context.load_seed("jupiter.py")
pmk.context.load_seed("saturn.py")
pmk.context.load_seed("uranus.py")
pmk.context.load_seed("neptune.py")
pmk.context.load_seed("pluto.py")
pmk.context.load_seed("blackhole.py")


class earth(PmkSeed.Seed):

    def __init__(self, context, poi=None):
        PmkSeed.Seed.__init__(self, context,poi)
        self.add_input_states("EARTH", type="X")
        self.add_output_states("MARS", type="X")
        pass

    def on_load(self):
        pass

    def run(self, pkt, data):

        name = str(data[0])
        fuel = int(data[1])
        fuel = fuel - 100

        print name+" returned to earth, fuel: "+str(fuel)
        print "payload: "
        for x in range(2,len(data)):
            print data[x]

        pass

e_seed = pmk.context.load_class(earth)

payload = e_seed.add_msg_item(None, "New Horizon")
payload = e_seed.add_msg_item(payload, 3000)
G = pmk.context.get_state_network()
#G.remove_node("DEV:X:5THDIMENSION")

e_seed.dispatch(None, payload, "MARS", automaton=G)



