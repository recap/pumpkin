###START-CONF
##{
##"object_name": "collector",
##"object_poi": "qpwo-2345",
##"auto-load": true,
##"remoting" : false,
##"parameters": [
##                 {
##                      "name": "tweet",
##                      "description": "",
##                      "required": true,
##                      "type": "TweetString",
##                      "format": "",
##                      "state" : "POSITIVE|NEGATIVE"
##                  }
##              ],
##"return": [
##
##
##          ] }
##END-CONF

import re, datetime
from pumpkin import PmkSeed

class collector(PmkSeed.Seed):

    def __init__(self, context, poi=None):
        PmkSeed.Seed.__init__(self, context,poi)
        self.total_counter = 0
        self.reset_counter()

    def on_load(self):
        print "Loading: " + self.__class__.__name__
        self.output_file = open("/home/pumpkin/tweetstats.data", 'w')
        self.date = None

    def reset_counter(self):
        self.pos_counter = 0
        self.neg_counter = 0
        self.current_date = None

    def run(self, pkt, tweet):
        new_date = re.search('T\w*(\s+)(.*)(\n)', tweet).group(2)
        self.current_date = datetime.datetime.strptime(new_date, "%Y-%m-%d %H:%M:%S")
        if self.date == None:
            # self.logger.info("self.date == None")
            self.date = self.current_date
        reset_counter = False
        # self.logger.info("distance: " + str(abs((self.date - self.current_date).total_seconds())) + " " + str(self.date) + " " + str(self.current_date))
        if self.date == None or abs((self.date - self.current_date).total_seconds()) > 10:
            # self.logger.info("new current date")
            self.date = self.current_date
            reset_counter = True
        self.total_counter += 1
        stag = self.get_last_stag(pkt)
        if stag == "POSITIVE":
            self.pos_counter += 1
        if stag == "NEGATIVE":
            self.neg_counter += 1

        if reset_counter:
            self.logger.info("reset counter")
            self.output_file.write("\"%s\" %d %d\n" % (self.date.strftime("%Y-%m-%d %H:%M:%S"), self.pos_counter, self.neg_counter))
            self.output_file.flush()
            self.reset_counter()

        # self.logger.info("Total: "+str(self.total_counter)+", Positive: "+str(self.pos_counter)+", Negative: "+str(self.neg_counter))

