__author__ = 'reggie'
__author__ = 'reggie'

###START-CONF
##{
##"object_name": "tweetreinject",
##"object_poi": "qpwo-2345",
##"parameters": [
##
##              ],
##"return": [
##              {
##                      "name": "tweet",
##                      "description": "raw tweet",
##                      "required": true,
##                      "type": "TweetString",
##                      "format": "",
##                      "state" : "RAW"
##                  }
##
##          ] }
##END-CONF





from pumpkin import *

import zmq

class tweetreinject(PmkSeed.Seed):

    def __init__(self, context, poi=None):
        PmkSeed.Seed.__init__(self, context,poi)
        self.zmq_cntx = zmq.Context()
        self.sock = self.zmq_cntx.socket(zmq.PULL)
        #self.sock.setsockopt(zmq.SUBSCRIBE, '')
        self.sock.connect("tcp://elab.lab.uvalight.net:7885")
        pass

    def on_load(self):
        print "Loading: " + self.__class__.__name__

        #print os.getcwd()
        #self._pre()

        pass

    def run(self, pkt):
        while 1:
            tweet = self.sock.recv()
            #log.debug("receiving..."+tweet)
            self.dispatch(pkt,tweet,"RAW")
        #dir = "./examples/tweeter/"
        #onlyfiles = [ f for f in listdir(dir) if isfile(join(dir,f)) ]
        #for fl in onlyfiles:
        #    fullpath = dir+fl
        #    if( fl[-3:] == "txt"):
        #        print "File: "+str(fl)
        #        tweet = ""
        #        with open(fullpath) as f:
        #            for line in f:
        #                if line.startswith('T'):
        #                    tweet = line
        #                if line.startswith("U"):
        #                    tweet = tweet + line
        #                if line.startswith("W"):
        #                    tweet = tweet + line
        #                    #self.__testfilter(tweet)
        #                    #self.dispatch(pkt, tweet,"RAW")
        #                    self.publish(tweet)


        pass

    def publish(self, tweet):
        #log.debug("Sending...")
        self.sock.send(tweet)

    #def _pre(self):
    #    nltk.data.path.append(os.getcwd()+"/examples/tweeter/nltk_data")
    #    pass
    #def is_english(self, text):
    #
    #    #nltk.download(nltk.corpus.stopwords)
    #
    #    ENGLISH_STOPWORDS = set(nltk.corpus.stopwords.words('english'))
    #    NON_ENGLISH_STOPWORDS = set(nltk.corpus.stopwords.words()) - ENGLISH_STOPWORDS
    #
    #    text = text.lower()
    #    words = set(nltk.wordpunct_tokenize(text))
    #    return len(words & ENGLISH_STOPWORDS) > len(words & NON_ENGLISH_STOPWORDS)
    #
    #def is_a(self,text):
    #    m = re.search('([A-Z]+[A-Za-z]+\s*[A-Za-z]*\s(has an|has a)\s[A-Z]+[A-Za-z]+)', text, re.S)
    #    if m:
    #        tw = m.group(0)
    #        return tw
    #
    #def __testfilter(self, tweet):
    #
    #
    #    m = re.search('W(\s+)(.*)(\n)', tweet, re.S)
    #    if m:
    #        tw = m.group(2)
    #        self.is_a(tw)
    #
    #    pass
    #
    #def is_haiku(self, text):
    #
    #    text_orig = text
    #    text = text.lower()
    #    if filter(str.isdigit, str(text)):
    #        return False
    #    words = nltk.wordpunct_tokenize(re.sub('[^a-zA-Z_ ]', '',text))
    #    #print words
    #    syl_count = 0
    #    word_count = 0
    #    haiku_line_count = 0
    #    lines = []
    #    d = cmudict.dict()
    #
    #    for word in words:
    #        if word.lower() in d.keys():
    #            syl_count += [len(list(y for y in x if isdigit(y[-1]))) for x in
    #                    d[word.lower()]][0]
    #        if haiku_line_count == 0:
    #            if syl_count == 5:
    #                lines.append(word)
    #                haiku_line_count += 1
    #        elif haiku_line_count == 1:
    #            if syl_count == 12:
    #                lines.append(word)
    #                haiku_line_count += 1
    #        else:
    #            if syl_count == 17:
    #                lines.append(word)
    #                haiku_line_count += 1
    #
    #    if syl_count == 17:
    #        try:
    #            final_lines = []
    #
    #            str_tmp = ""
    #            counter = 0
    #            for word in text_orig.split():
    #                str_tmp += str(word) + " "
    #                if lines[counter].lower() in str(word).lower():
    #                    final_lines.append(str_tmp.strip())
    #                    counter += 1
    #                    str_tmp = ""
    #            if len(str_tmp) > 0:
    #                final_lines.append(str_tmp.strip())
    #            return True
    #
    #        except Exception as e:
    #            print e
    #            return False
    #    else:
    #        return False
    #
    #    return True
