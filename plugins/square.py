__author__ = 'reggie'


import DRPlugin

from time import sleep


class square(DRPlugin.PluginBase):
    def on_load(self):
        print "Loading: " + self.__class__.__name__
        self.poi =" qaz-456"
        pass

    def run(self, *args):
        print "Running: " + self.__class__.__name__
        sqr = 0
        if(len(args) > 0 ):
            sqr = int(args[0]) * int(args[0])

        return sqr
        pass
    

    def on_unload(self):
        print "Unloading: " + self.__class__.__name__
        pass
