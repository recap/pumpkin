import os, time
import pumpkin
import ujson as json
from loremipsum import get_sentences
from pumpkin import PmkSeed


class master(PmkSeed.Seed):

    def __init__(self, context, poi=None):
        PmkSeed.Seed.__init__(self, context,poi)
        self.add_input_states("MASTER")
        self.add_output_states("SLAVE")
        pass

    def on_load(self):
        self.master_dict = {}
        self.counter = 0
        pass

    def run(self, pkt, data):
        dict = json.loads(str(data[0]))
        self.update_master_dict(dict, self.master_dict)
        self.counter += 1
        pass

    def update_master_dict(self, sdict, mdict):
        for k in sdict.keys():
            if k in mdict.keys():
                mdict[k] = mdict[k] + sdict[k]
            else:
                mdict[k] = sdict[k]

    def dump_dict(self, dict=None):
        if dict == None:
            dict = self.master_dict
        for k in dict.keys():
            if dict[k] > 10:
                print k+" "+str(dict[k])


pmk = pumpkin.initialize_pumpkin(cli=False)

pmk.context.load_seed("slave.py")

master_seed = pmk.context.load_class(master)

for i in range(0,1000):
    sentences_list = get_sentences(500)

    whole_text = ""
    for s in sentences_list:
        whole_text += s

    master_seed.dispatch(None, whole_text, "SLAVE")

a = 10
while True:
    c = master_seed.counter
    if c > a:
        a += 10
        master_seed.dump_dict()

    time.sleep(2)





