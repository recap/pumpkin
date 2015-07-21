__author__ = 'reggie'

###START-CONF
##{
##"object_name": "saturn",
##"object_poi": "my-saturn-1234",
##"auto-load" : true,
##"parameters": [ {
##                  "name": "name",
##                  "description": "name to saturn",
##                  "required": true,
##                  "type": "String",
##                  "state" : "SATURN"
##              } ],
##"return": [
##              {
##                  "name": "greeting",
##                  "description": "a greeting",
##                  "required": true,
##                  "type": "String",
##                  "state" : "URANUS|EARTH"
##               }
##
##          ] }
##END-CONF




from pumpkin import *

class saturn(PmkSeed.Seed):

    def __init__(self, context, poi=None):
        PmkSeed.Seed.__init__(self, context,poi)
        pass


    def run(self, pkt, data):
        """ Data is transformed at intermediate points on its way
        to a destination. In this case we are simply adding
        "hello" to a name to form a greeting. This will be
        dispatched and received by a collector.
        """

        name = str(data[0])
        fuel = int(data[1])
        fuel = fuel - 200


        print name+" leaving saturn orbit fuel: "+str(fuel)
        payload = self.add_msg_item(None, name)
        payload = self.add_msg_item(payload, fuel)
        for i in range(2,len(data)):
            payload = self.add_msg_item(payload, data[i])
        payload = self.add_msg_item(payload, self.get_id())

        if fuel > 800:
            self.dispatch(pkt, payload, "URANUS")
        else:
            self.dispatch(pkt, payload, "EARTH")
        pass

