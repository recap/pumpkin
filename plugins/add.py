__author__ = 'reggie'


import DRPlugin

from time import sleep


class add(DRPlugin.PluginBase):
    def on_load(self):
        print "Loading: " + self.__class__.__name__
        self.poi = "qaz-123"
        pass

    def run(self, *args):
        print "Running: " + self.__class__.__name__
        sp = args[0].split(",")


        count = 0
        for s in sp:
            count = count + int(s)

        return count
        pass


    def on_unload(self):
        print "Unloading: " + self.__class__.__name__
        pass
