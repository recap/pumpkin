__author__ = 'reggie'

###START-CONF
##{
##"object_name": "pluto",
##"object_poi": "my-pluto-1234",
##"auto-load" : true,
##"parameters": [ {
##                  "name": "name",
##                  "description": "name to pluto",
##                  "required": true,
##                  "type": "X",
##                  "state" : "PLUTO"
##              } ],
##"return": [
##              {
##                  "name": "greeting",
##                  "description": "a greeting",
##                  "required": true,
##                  "type": "X",
##                  "state" : "HELIOSPHERE|EARTH"
##               }
##
##          ] }
##END-CONF




from pumpkin import *

class pluto(PmkSeed.Seed):

    def __init__(self, context, poi=None):
        PmkSeed.Seed.__init__(self, context,poi)
        pass


    def run(self, pkt, data):


        name = str(data[0])
        fuel = int(data[1])
        fuel = fuel - 1000


        print name+" leaving pluto orbit fuel: "+str(fuel)
        payload = self.add_msg_item(None, name)
        payload = self.add_msg_item(payload, fuel)
        for i in range(2,len(data)):
            payload = self.add_msg_item(payload, data[i])
        payload = self.add_msg_item(payload, self.get_id())

        if fuel > 10000:
            self.dispatch(pkt, payload, "HELIOSPHERE")
        else:
            self.dispatch(pkt, payload, "EARTH")
        pass