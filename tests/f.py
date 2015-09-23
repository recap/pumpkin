import os, time
import pumpkin
from pumpkin import PmkSeed


pmk = pumpkin.initialize_pumpkin(cli=False)

pmk.context.load_seed("g.py")
# pmk.context.load_seed("jupiter.py")
# pmk.context.load_seed("saturn.py")
# pmk.context.load_seed("uranus.py")
# pmk.context.load_seed("neptune.py")
# pmk.context.load_seed("pluto.py")
# pmk.context.load_seed("blackhole.py")


class f(PmkSeed.Seed):

    def __init__(self, context, poi=None):
        PmkSeed.Seed.__init__(self, context,poi)
        self.add_input_states("S0", type="X")
        self.add_output_states("S1", type="X")
        pass

    def on_load(self):
        pass

    def run(self, pkt, data):


        pass

e_seed = pmk.context.load_class(f)


G = pmk.context.get_state_network()
#G.remove_node("DEV:X:5THDIMENSION")
# print "dispatch"
e_seed.dispatch(None, None, "S1", automaton=G)



