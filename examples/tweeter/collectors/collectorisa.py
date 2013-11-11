__author__ = 'reggie'


###START-CONF
##{
##"object_name": "collectorisa",
##"object_poi": "qpwo-2345",
##"parameters": [
##                 {
##                      "name": "tweet",
##                      "description": "english haiku tweets",
##                      "required": true,
##                      "type": "TweetString",
##                      "format": "",
##                      "state" : "ISA"
##                  }
##              ],
##"return": [
##
##          ] }
##END-CONF




from pumpkin import PmkSeed




class collectorisa(PmkSeed.Seed):

    def __init__(self, context, poi=None):
        PmkSeed.Seed.__init__(self, context,poi)
        self.d = None
        pass

    def on_load(self):
        print "Loading: " + self.__class__.__name__

        pass


    def run(self, pkt, tweet):

        print "ISA: "+str(tweet)

        pass

