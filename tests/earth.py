import os, time
import pumpkin
from loremipsum import get_sentences
from pumpkin import PmkSeed


pmk = pumpkin.initialize_pumpkin(cli=False)

#pmk.context.load_seed("mars.py")
#pmk.context.load_seed("jupiter.py")
pmk.context.load_seed("saturn.py")
pmk.context.load_seed("uranus.py")
pmk.context.load_seed("neptune.py")
pmk.context.load_seed("pluto.py")
pmk.context.load_seed("blackhole.py")



class earth(PmkSeed.Seed):

    def __init__(self, context, poi=None):
        PmkSeed.Seed.__init__(self, context,poi)
        self.add_input_states("EARTH")
        self.add_output_states("MARS")
        pass

    def on_load(self):
        #payload = self.add_msg_item(None, "New Horizon")
        #payload = self.add_msg_item(payload, 3000)
        #self.dispatch(None, payload, "MARS")

        pass

    def run(self, pkt, data):
        """ Data is transformed at intermediate points on its way
        to a destination. In this case we are simply adding
        "hello" to a name to form a greeting. This will be
        dispatched and received by a collector.
        """
        name = str(data[0])
        fuel = int(data[1])
        fuel = fuel - 100

        print name+" returned to earth, fuel: "+str(fuel)
        print "payload: "
        for x in range(2,len(data)):
            print data[x]

        #self.dispatch(pkt, payload, "MARS")
        pass

e_seed = pmk.context.load_class(earth)

payload = e_seed.add_msg_item(None, "New Horizon")
payload = e_seed.add_msg_item(payload, 3000)
G = pmk.context.get_state_network()
G.remove_node("DEV:String:5THDIMENSION")

#e_seed.dispatch(None, payload, "MARS", automaton=G)


sentences_list = get_sentences(50)
for s in sentences_list:
    print s



#G = pmk.context.get_state_network()
#print G.edges()
#print "####"
#print G.nodes()
#print "%%%%%%%"
#time.sleep(30)
#G.remove_node("DEV:String:5THDIMENSION")
#G2 = pmk.context.get_state_network()
#print G.edges()
#print "$$$$$$$$$$$$"
#print G2.edges()
#print G.edges()
#print "22222####2222"
#print G.nodes()
#for n in G.nodes():
#    print n


#class ends(PmkSeed.Seed):
#
#    def __init__(self, context, poi=None):
#        PmkSeed.Seed.__init__(self, context, poi)
#        print "Loaded"
#        self.add_input_states("WORLD_GREETING")
#        self.add_output_states("UNGREETING")
#        pass

#    def on_load(self):
#        self.dispatch(None, "Mercury", "UNGREETED")
#        self.dispatch(None, "Venus", "UNGREETED")
#        self.dispatch(None, "World", "UNGREETED")
#        self.dispatch(None, "Mars", "UNGREETED")
#        self.dispatch(None, "Jupiter", "UNGREETED")
#        self.dispatch(None, "Saturn", "UNGREETED")
#        self.dispatch(None, "Uranus", "UNGREETED")
#        self.dispatch(None, "Neptune", "UNGREETED")


#    def run(self, pkt, data):
#        """ Data should be sourced here an injected into the
#        data transformation network. In this example we are
#        inject a single data "world" to a greeter which will
#        tell us "hello"
#        """

#        greeting = str(data[0])
#        print "Returned greeting: "+ str(greeting)
#
#        pass

#pmk.load_class(ends)